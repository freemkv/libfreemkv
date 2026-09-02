# `src/mux/mp4/mod.rs` — progressive MP4 muxer notes

Long-form rationale relocated from source comments (see `ci/comment-guard.py`
audience caps). Each section is pointed to from a short `//` comment at its
call site.

## Module overview

Progressive MP4 (ISO-BMFF) muxer for `mp4://`. Writes `ftyp` + `moov` +
`mdat` with faststart on by default: a `moov`-sized hole is reserved between
`ftyp` and `mdat` at the start (so sample offsets are fixed and never
rewritten), sample data streams into `mdat`, and at `finish()` the `moov`
index is written into the reserved hole with a trailing `free` box for the
slack. If the estimate is blown, it falls back to moov-at-end. Unlike the
fragmented `fmp4` sibling (DASH init+moof/mdat), this is a single
self-contained file — the shape people mean by "an mp4" — and moov-first
means it streams over HTTP without a pre-fetch.

### Track model

One video track (HEVC / H.264) plus every audio track whose codec has a
clean MP4 mapping (AC-3 → `ac-3`/`dac3`, E-AC-3 → `ec-3`/`dec3`, DTS/DTS-HD →
`dtsc`/`dtsh`/`ddts`). This is the fit oracle: a codec MP4 can't carry
(TrueHD, LPCM) or that has no sample entry here is excluded, never silently
dropped — `fit_report` lets the CLI enumerate exactly what was left out and
why. Video NALs pass through unchanged (the demux hands us length-prefixed
hvcC/avcC framing — already MP4's form). Decode timestamps are derived (the
pipeline carries presentation PTS only): video is constant-frame-rate on
disc, so a constant decode duration + signed `ctts` reproduces the B-frame
reorder exactly; audio has no reorder, so per-sample durations come straight
from the PTS deltas.

Reference: ISO/IEC 14496-12 (ISO base media file format), 14496-15
(avcC/hvcC).

## `faststart_fits`

Whether `gap` bytes of leftover reserved-hole slack (after `moov` is written
into it) can be closed: an exact fill (`0`) needs no `free` box at all, and
`8+` bytes is enough to hold one (an ISO-BMFF box header is 8 bytes: 4-byte
size + 4-byte type). A gap of 1–7 bytes cannot be expressed as any box, so
`Mp4Sink::finish` must fall back to moov-at-end instead of writing a `free`
box that lies about its own size.

## `Mp4Sink::final_report`

This exists because the plan is a PREDICTION. `finish()` drops an audio
track no frame of which yielded a parseable sample entry (it cannot be
described in `stsd`) and returns `Ok` so an export whose video is fine still
succeeds — but then the plan, which is the only structured report the crate
publishes, still named that stream as carried. A caller believing it, reports
a successful export of a file with no audio. Ask this after `finish()`
before telling anyone what was written.

## `Mp4Sink::undelivered_streams`

The driver surfaces this as `MuxOutcome::undelivered_streams` so the caller
learns programmatically that the file is missing a stream the pre-mux plan
promised, instead of only in a log line.

## `STD_RATES`

The order of this table is NOT significant: `detect_rate` picks the entry
nearest the measured rate, so a new rate may be appended anywhere without
shadowing an existing one.

## `RATE_TOLERANCE_FPS`

Half an fps separates every neighbouring pair in the table (23.976/24 are
0.024 apart, so both fall inside one another's window — which is exactly why
the match must be nearest-wins, not first-wins).

Two mutants in `detect_rate`'s snapping loop — `d < RATE_TOLERANCE_FPS` and
the tie-break `d < best_d`, each flipped to `<=` — are not closed by any test
here, and are believed unreachable rather than merely untested: both require
an f64 distance computed as `(fps - rate).abs()` to land on EXACTLY `0.5`, or
on an exact tie between two candidate distances, where `fps` is `1e9 /
median` for an INTEGER nanosecond `median`. A brute-force search (median
from 1 to 1e8 ns, every `STD_RATES` entry) found no integer median whose
measured distance is bit-exact `0.5`, nor one producing an exact tie between
neighbouring entries: the target real number (e.g. `1e9 / 24.5`) is never
itself an integer, so no integer median's true quotient rounds to a double
that is bit-identical to the boundary. If a future change makes either
boundary reachable (e.g. by accepting a caller-supplied `median` directly
instead of deriving it from PTS deltas), revisit this.

## `video_colr`

The code points come from `crate::mux::mkv::cicp_for_video` — the single
resolver EVERY sink shares (measured bitstream CICP first, then the coarse
`ColorSpace` enum with the HDR-driven transfer override). This box must never
carry its own copy of that mapping: the copy that used to live here had
drifted to hardcode transfer 16 (SMPTE ST 2084 / PQ) for all BT.2020 —
tagging an HLG title, whose transfer is 18 (ARIB STD-B67), as PQ — and
transfer 6 (BT.601) for BT.470 System B/G, whose transfer is 5. Both
disagreed with the MKV sink and the FVI sidecar for the same disc.

## Test: `a_video_track_with_no_resolved_resolution_is_an_error_not_a_zero_sized_track`

ISO/IEC 14496-12 makes width and height mandatory in both `tkhd` (8.3.2) and
VisualSampleEntry (12.1.3), so unlike Matroska — which simply omits the
optional PixelWidth/PixelHeight elements — MP4 has nothing to leave out.
Writing zeros produces a structurally complete file that passes every
container check and that no player can render, with no error anywhere: a
wrong answer that looks like a successful rip.

`Resolution::pixels()` returns `Option` for this reason. It used to return a
fabricated 1920x1080, then `(0, 0)`; the zero pair reads as a usable value,
so this sink stored it and serialised it, and the guard that two of the
three sinks have was never needed here and so was never written.

## `find_child` (test helper)

A minimal box walker duplicated here for tests — the reader's own
box-walking (`find_box`) lives in `read.rs` and is private to that module.

## Test: `audio_track_with_no_parseable_sample_entry_is_dropped_not_emitted_empty`

An audio track whose frames never yield a parseable sample entry must be
dropped from moov, NOT written as an stsd declaring entry_count=1 around an
empty entry — that is a structurally invalid mp4 returned as success.

The video track must survive, and no audio frame may be lost from mdat on
the way: `write()` previously returned `Ok(())` without recording the
sample, so leading audio frames vanished silently.

Mutation check: restore `write()`'s early `return Ok(())` and the
unparseable bytes never reach mdat; drop `finish()`'s describability retain
and moov gains a second trak carrying an empty sample entry.

## Test: `dropped_audio_track_is_reported_not_just_logged`

Dropping the undescribable audio track keeps the export succeeding (its
video is fine), but the crate must not then keep CLAIMING that stream:
`mp4_fit_report` — the only structured report — still lists it as included,
so a caller printing the plan reports a successful export of a file with no
audio at all.

`final_report()` must therefore describe the FILE (the stream moved to
`skipped` with `UndescribableAudio`), and `undelivered_streams()` — which
the driver folds into `MuxOutcome::undelivered_streams` — must name it so
the loss is programmatic, not just a log line.

Mutation check: stop recording the drop in `finish()` and the plan and the
file disagree again with nothing but a `tracing::warn` between them.

## Test: `mvhd_next_track_id_exceeds_every_retained_track_id`

`mvhd.next_track_id` must EXCEED every track_ID in the file (ISO/IEC
14496-12 §8.2.2). It was derived from the retained track COUNT, so a drop at
`finish()` made it collide with a live id: ids [1, 3] retained → count 2 →
next_track_id 3, which is track 3. A tool appending a track with that id
creates a duplicate.

## Test: `pack_language_packs_three_lowercase_letters_into_15_bits`

Every bit-twiddle in `pack_language`'s valid-input path (both shift amounts,
both `-0x60` subtractions on the first two letters, and the OR that
assembles them) pinned against hand-computed packed values, using letters
other than 'a' so a `-`↔`/` flip on the per-letter offset is visible:
`'a' - 0x60 == 1 == 'a' / 0x60`, so a fixture starting with 'a' cannot tell
subtraction from division apart on that letter.

Three mutants of `pack_language` are NOT killed by this test, or by any
other — they are equivalent, proven by construction rather than merely
unobserved:

1. `(b[0] - 0x60) as u16` → `(b[0] + 0x60) as u16` on the FIRST letter (the
   one shifted `<< 10`). The two results differ by exactly `0x60 * 2 = 192 =
   3 * 64`, and multiplying by `1 << 10` then truncating to `u16` is
   arithmetic mod `65536`; since `192 * 1024 = 196_608 = 3 * 65536`, the `+`
   and `-` forms land on the identical `u16` for every possible byte, not
   just the ones this fixture picks. (The same swap on the SECOND letter,
   shifted only `<< 5`, is very much NOT equivalent — `192 * 32 = 6144` is
   not a multiple of 65536 — which is why that one IS caught above and only
   the first letter's `+` survives.)
2. Both `|` → `^` mutations that combine the three shifted fields. The three
   components are each a lowercase letter minus `0x60`, i.e. in `1..=26`,
   which fits in 5 bits (`0..=31`); shifted by `0`, `5` and `10` they occupy
   disjoint bit ranges for every valid input, and OR and XOR agree exactly
   when their operands share no set bit.

## Test: `estimate_reserve_uses_the_streams_own_fps_not_the_24fps_fallback`

`estimate_reserve`'s per-stream fps comes from `v.frame_rate.as_fraction()`
only when BOTH `n > 0 && d > 0`; otherwise it falls back to a flat 24.0.
`FrameRate::Unknown` is the only variant with `n == 0` (it reports `(0,
1)`), so it is the one real input that takes the fallback branch — every
other variant has `n, d > 0` and must use its OWN fps, not 24.0.

A film-rate title (23.976 fps) is the fixture that can tell "used its own
fps" apart from "used the 24.0 fallback": at 24 fps flat those two numbers
coincide, so a guard broken by an operator flip (`&&`→`||`, `>`→`==`/`<`/
`>=`) would be invisible on it. Duration 76_500 s is chosen so the two
answers land in different 4 MiB reserve grains, not just differ before
rounding.

## Test: `estimate_reserve_unknown_frame_rate_falls_back_to_24fps_not_zero`

The complementary case: `FrameRate::Unknown` (n=0) is the one real input
meant to take the fallback branch. If the guard's `n > 0` is weakened so
zero passes it (`==0`, `>=0`) or the `&&` becomes `||`, the code computes `0
/ 1 = 0.0` fps instead of falling back to 24.0 — collapsing the estimate to
near-zero samples instead of a reasonable guess.

## Test: `estimate_reserve_models_dts_at_512_samples_per_frame_not_1536`

`estimate_reserve` models a DTS/DTS-HD-MA/DTS-HD-HR audio unit as 512
samples (a DTS core AU is `(nblks+1)*32`), a THIRD of the 1536-sample
(E-)AC-3 default the `_` arm uses for everything else. Deleting the DTS
match arm silently reverts every DTS track to the AC-3 model, which
under-reserves a DTS-heavy title's sample table 3x and can push a real mux
onto the moov-at-end fallback.

## Test: `tkhd_duration_is_seconds_times_movie_timescale_for_both_media_types`

`build_video_trak_full` and `build_audio_trak_full` both compute
`tkhd.duration = secs * MOVIE_TIMESCALE` — a SEPARATE multiplication from
the media-timescale duration in `mdhd`, because `tkhd.duration` lives in the
movie's 90 kHz clock (ISO/IEC 14496-12 §8.3.2). Read it straight out of the
emitted `tkhd` bytes (offset 28 in a version-1 tkhd: version+flags (4) +
creation(8) + modification(8) + track_id(4) + reserved(4) = 28), so the
assertion is on what the file says, not a second copy of the formula.

## Test: `audio_sample_durations_computes_exact_tick_deltas_and_repeats_the_last`

`audio_sample_durations`'s per-sample ticks are `ns * ts / NS`, and the
inter-sample delta is `ticks(next) - ticks(prev)` (clamped at 0), with the
LAST duration repeated for the trailing sample. PTS deltas are chosen so
every tick value is an exact integer and the two windows differ (32 ms then
another 32 ms from a non-zero base), so a `-`↔`+` flip on the delta is
visible on the second window even though it is invisible on the first
(whose previous tick is 0).

## Test: `audio_sample_durations_single_sample_uses_timescale_over_30_fallback`

The single-sample fallback (`durs.last()` is `None`) pushes `timescale /
30`, guarded by `!samples.is_empty()`. A single sample exercises both:
`windows(2)` yields nothing, so the fallback branch is the only source of a
duration at all.

## Test: `detect_rate_needs_at_least_two_samples_not_more`

Fewer than 2 samples can't measure a delta at all: fixed 90 kHz/3003
fallback (not 90 kHz/anything else, and not a panic on an empty median).
EXACTLY 2 samples is the boundary itself, not just "some samples fewer than
2" — a `<`→`==`/`<=` mutant on `samples.len() < 2` forces the fallback for 2
samples too, even though they carry a perfectly good 25 fps delta.

## Test: `detect_rate_filters_zero_deltas_not_just_negative_ones`

Only POSITIVE deltas are considered (`filter(|&d| d > 0)`). A `>`→`>=`
mutant lets zero deltas (duplicate/out-of-order PTS) through, which shifts
which element `deltas[deltas.len() / 2]` lands on. Three duplicate
timestamps plus one real 25 fps delta make the shift land on the zero
itself: median becomes 0, `fps` becomes `NS / 0 = inf`, no `STD_RATES` entry
is within tolerance of infinity, and the fallback path's `median` of 0
forces its duration floor of 1 — a completely different, clearly-wrong
answer from the correct (25, 1).

## Test: `detect_rate_fallback_duration_is_median_times_90khz_over_ns`

A rate with no nearby `STD_RATES` entry takes the fallback branch:
`timescale = 90_000`, `duration = (median * 90_000) / NS`. 5 fps (200
ms/frame) is chosen so `median * 90_000` is an exact multiple of `NS`,
giving a clean expected duration that a `*`↔`+`/`/` flip on either operator,
or `/`↔`%`, cannot coincidentally reproduce.
