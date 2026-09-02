# MKV muxer internals (`src/mux/mkv.rs`)

Overflow rationale relocated here by `ci/comment-guard.py` (long-form prose
that would otherwise exceed the internal-comment cap). Each section is
pointed to from the corresponding `//` comment in the source.

## Blu-ray 3D MVC BlockAddition mapping

`BLOCK_ADD_ID_TYPE_MVCC` is the `mvcC` BlockAddIDType — the
`MVCDecoderConfigurationRecord` fourcc, big-endian ASCII `'m''v''c''C'`.
Matroska BlockAdditionMapping/BlockAddIDType for a Blu-ray 3D MVC
configuration (RFC 9559 + Matroska Codec Specifications §4.1.5; equals
ISO/IEC 14496-15 `MVCConfigurationBox('mvcC')`). The per-frame dependent
(right-eye) view rides as a BlockAdditional under this mapping.

## MVC decoder config record layout

`mvc_decoder_config_record` builds an `MVCDecoderConfigurationRecord`
(ISO/IEC 14496-15:2013 §7.6.2) from the dependent view's subset-SPS (NAL
type 15) and PPS NAL units. This is the `BlockAddIDExtraData` for the
`mvcC` BlockAdditionMapping (and, in the ISO container, the
`MVCConfigurationBox('mvcC')` payload).

Layout mirrors the AVCDecoderConfigurationRecord except `byte[4]`
repurposes the AVC record's `bit(6) reserved` as
`complete_representation(1) | explicit_au_track(1) | reserved '1111'(4)`.
`profile`/`compat`/`level` describe the WHOLE MVC stream and come from the
subset SPS (bytes 1..=3). `length_size_minus_one` MUST match the base avcC
(freemkv always emits 4-byte length prefixes → 3). SPSs precede subset SPSs
in the array; here the array carries the dependent view's parameter sets.

Returns `None` if either param set is absent, too short, or exceeds the
16-bit length field (a param set > 65535 bytes is non-conforming and would
mis-frame the record).

## MVC CodecPrivate layout

`mvc_codec_private` builds the `CodecPrivate` for an MVC (Blu-ray 3D) base
track: the base view's `AVCDecoderConfigurationRecord` (`avcc`) followed by
an `mvcC` extension block, per the Matroska Codec Specifications §4.3.9:

```text
avcC ‖ u32be(extension_block_size − 4) ‖ "mvcC" ‖ MVCDecoderConfigurationRecord
```

The size field is the extension block length **excluding the 4-byte size
field itself** — i.e. `4 ("mvcC") + record.len()`. This is the track-level
MVC signal that decoders and media analyzers read (the per-frame
`BlockAdditional` under the `mvcC` BlockAdditionMapping carries the
dependent view's data). A plain (2D) track never calls this — it writes its
`avcc` verbatim.

## CICP colour resolution precedence

`cicp_for_video` resolves a video stream's CICP colour code points —
`(matrix, transfer, primaries, range)`, ITU-T H.273 — using a single
precedence so EVERY sink (the MKV muxer here AND the FVI sidecar in
`videomap.rs`) agrees and can never drift:

1. **Measured CICP** read from the bitstream (HEVC/H.264 VUI
   `colour_description` or MPEG-2 `sequence_display_extension`) is
   AUTHORITATIVE — copied through verbatim when present.
2. Otherwise fall back to the coarse `color_space` enum (a playlist nibble /
   PAL-NTSC guess), THEN apply the HDR-driven transfer override: BT.2020
   only appears on HDR UHD, where the real transfer is PQ (16) for
   HDR10/HDR10+/DV or HLG (18) for HLG — never the SDR transfer 14 the enum
   alone would emit.

## HDR10 unit conversions and SEI primary order

`write_hdr10` emits the HDR10 static-metadata children of the Matroska
`Colour` element: `MasteringMetadata` (chromaticity / luminance floats)
plus `MaxCLL` / `MaxFALL` (uints). Called ONLY when the metadata was
measured from the bitstream SEI, so it is never written for SDR content.

Unit conversions (Rec. ITU-T H.265 D.3.28 → RFC 9559 / Matroska):
- chromaticity: SEI integer × 0.00002 → Matroska float in [0, 1]
- luminance:    SEI integer × 0.0001  → Matroska float in cd/m²
- MaxCLL / MaxFALL: already cd/m² integers → written as uints verbatim

The SEI primary order is c=0 Green, c=1 Blue, c=2 Red (D.3.28); the
`Hdr10Metadata` arrays preserve that SEI order, so index 0 → G, 1 → B,
2 → R is mapped onto the Matroska R/G/B element layout here.

## Language element default rationale

`language_or_und` is the Matroska `Language` element the muxer writes for
a stream whose source reported `lang`. RFC 9559 §12 defines the element as
an ISO 639-2 code and gives no meaning to an empty one; the code for "no
language stated" is `und`, and that is what a source with no language
table (the HD-DVD EVO stream probe, a Blu-ray STN slot with no language
bytes) has to emit. The element is written unconditionally by
`MkvMuxer::new`, so this is the one place that decides it — a source-side
default would have to be repeated in every scanner and would still leave
the muxer able to ship an invalid file.

## Why BlockGroup buffering avoids per-frame seeks

`MkvMuxer::block_group_buf` is a reusable scratch buffer for assembling
ONE BlockGroup before it is written with a single `write_all`.

The BlockGroup path is the hot one — the MPEG-2 parser stamps a per-frame
duration, so every I/P/B frame lands there, plus AC-3 audio and PGS
subtitles (~350k BlockGroups for a 90-minute feature). Building the
element in memory means its size is known before it reaches the file, so
no `start_master`/`end_master` back-patch is needed: that removes the
per-frame `stream_position()` + two `seek()`s, each of which flushed the
4 MiB `BufWriter` (`BufWriter` does not override `stream_position`, so a
position query is `seek(Current(0))` = flush + lseek) and reset the
writeback pipeline's `last_flush_pos`. Kept on the muxer so the allocation
is made once, not per frame.

## dropped_pre_cluster logging rationale

`MkvMuxer::dropped_pre_cluster` counts frames handed to `write_frame` that
were dropped because no cluster was open yet (a cluster only opens on a
keyframe from the primary video track, whatever index that is — not
necessarily track 0). See `write_frame` for the cluster-driver invariant.

The ALL-dropped case is surfaced as an error by `finish()`, but via
`frame_count == 0`, not via this counter. A PARTIAL drop — leading audio /
subtitle frames ahead of the first video IDR, or an M2TS whose PMT lists
audio before video — is normal enough not to fail the mux, but it used to
leave NO record anywhere: the field was incremented at two sites and read
nowhere (no log, no error, no accessor), so those frames vanished from the
output with `completed = true`, an empty `undelivered_streams`, and
nothing in the log. `finish()` now logs the count — that log is the
field's only reader, so do not delete it and turn this back into dead
bookkeeping.

## TimestampScale choice

`TIMESTAMP_SCALE_NS`: nanoseconds per Matroska timestamp tick, 0.1 ms
(100_000 ns).

The classic 1 ms scale truncates two distinct cadences onto the same tick:
- 23.976 fps video frames are ~41.7 ms apart, but with B-frame reorder two
  neighbouring frames can round to the same whole millisecond — a decoder
  then derives colliding DTS ("non monotonically increasing dts").
- TrueHD audio access units are 0.833 ms (1/1200 s); at 1 ms granularity
  every AU truncates to a 1 ms grid and the per-track monotonic nudge has
  to space them at a fabricated 1 ms instead of their true 0.833 ms.

0.1 ms resolves both: 41.7 ms and 0.833 ms each map to distinct ticks, so
frames stop colliding and audio keeps its real cadence. Player/parser
support for sub-millisecond TimestampScale is universal (it is the spec
default mechanism). The cost is a smaller per-cluster i16 span (see
`MAX_BLOCK_REL`), handled by splitting clusters and emitting a Cue for the
split (see `write_frame`).

## cluster duration vs. i16 block-relative limit

`CLUSTER_DURATION_TICKS`: nominal new-cluster interval (2 s) expressed in
TimestampScale ticks.

A keyframe only OPENS a new cluster once this much has elapsed since the
open cluster's timestamp, so the actual cluster span runs from this value
up to roughly this value plus one GOP (the next keyframe lands a GOP
later). With a typical ≤ 1 s GOP that worst-case span (~3 s ≈ 30_000
ticks) stays UNDER the i16 block-relative limit (`MAX_BLOCK_REL` = 32_767
ticks ≈ 3.27 s at the 0.1 ms scale), so video keyframes drive Cue-aligned
cluster boundaries and the i16-overflow split path stays a rare fallback
(long audio-only stretches or pathological multi-second GOPs) rather than
the common case. The classic 5 s window would, at this scale, force an
unaligned i16 split inside every cluster.

## MAX_BLOCK_REL / MIN_BLOCK_REL rationale

`MAX_BLOCK_REL` is the maximum block-relative timestamp expressible in the
signed 16-bit SimpleBlock/Block field (`i16::MAX` ticks). A frame whose
offset from the open cluster's timestamp falls outside
`i16::MIN..=i16::MAX` ticks forces a new cluster (see `write_frame`) so
the `as i16` cast can never wrap — in EITHER direction. PES timestamps
come from untrusted disc/file bytes and can back-jump on discontinuities,
so the lower bound matters as much as the upper one. At a 0.1 ms scale
i16::MAX is ~3.27 s, well under the 5 s cluster window, so a long-GOP /
audio-only stretch can hit this bound before the keyframe boundary — the
split path must (and does) push a Cue. `MIN_BLOCK_REL` is the
corresponding lower bound (`i16::MIN`).

## monotonic_ts non-monotonic DTS rationale

`monotonic_ts` forces a per-track block timestamp (in TimestampScale
ticks) to be strictly later than the previous one written for that track.
`prev` is the last timestamp for the track (`None` for the first frame).
Fixes non-monotonic DTS: some audio PES PTS truncate to the same tick as
the prior frame (or tick back one from rounding), which strict
players/decoders reject. At the 0.1 ms scale a TrueHD AU (0.833 ms = ~8
ticks) no longer collides with its neighbour, so this rarely fires for
lossless audio — but a +1-tick nudge (0.1 ms, sub-AU and inaudible) still
guards genuine same-tick collisions on any no-reorder track. Never moves a
timestamp earlier.

## block_ts rationale

`block_ts` computes the per-track block timestamp. The strictly-monotonic
nudge is applied to AUDIO/SUBTITLE tracks only; ALL VIDEO tracks are
returned UNCHANGED.

With B-frames, a video frame's presentation PTS is legitimately
non-monotonic in decode/storage order (a B-frame sits between its
anchors, below the frame stored just before it). Forcing it
strictly-increasing clobbers those PTS to prev+1ms — a `copy` remux
preserves the (wrong) value, but a decoder derives DTS from the HEVC POC
and finds them colliding ("non monotonically increasing dts", thousands
per title). Matroska SimpleBlock permits non-monotonic block timestamps
(signed block-relative offsets), so video keeps its true PES PTS; only
no-reorder tracks (audio, subtitles), where a same-millisecond collision
IS a real defect, get nudged.

The exemption is keyed on `is_video` (track type), NOT a track index: a
title can carry more than one video track — e.g. a Dolby Vision
enhancement layer at index 1 — and every one must keep its true PTS.
Keying on `track_idx == 0` clamped the EL and reintroduced the exact
non-monotonic-DTS warning this exemption exists to prevent.

## track_vint widths

`track_vint` encodes a Matroska track number as an EBML VINT into a stack
buffer, returning the buffer and the used length. Track numbers are small
(1-based, a handful of tracks), so 1 byte covers `< 0x80`, 2 bytes covers
`< 0x4000`, and 3 bytes covers `< 0x20_0000`; no heap allocation, called
once per block on the mux hot path.

Each width uses a marker bit that must NOT collide with the payload's top
byte: the 1-byte marker is 0x80 (7 payload bits), the 2-byte marker 0x40
(14 payload bits), the 3-byte marker 0x20 (21 payload bits). Handling all
three in RELEASE (not just `debug_assert`) means an out-of-2-byte-range
track number can never silently clobber the marker bit and corrupt the
block. Real discs never approach even the 2-byte range; the 21-bit
ceiling is an absurd upper bound kept as a `debug_assert`.

## codecPrivate path history

Pre-0.13 had a deferred codecPrivate path (placeholder + seek-back fill)
for video, but the PES pipeline hands it up-front via `DiscTitle`, so that
path was never exercised — removed in the 0.13 dead-code sweep.

## set_clips seam-correction rationale

`MkvMuxer::set_clips` drives seam correction from the title's PlayItem
marks instead of inferring it from PTS jumps.

A multi-clip Blu-ray playlist joins its clips with overlaps and skips
that PTS inspection cannot recover: a forward jump is indistinguishable
from frames lost to damaged media, and an overlap smaller than the
reorder threshold is invisible. Given the marks, each clip is placed at
the sum of the earlier clips' durations, so the output runs exactly as
long as the playlist says the title is.

No-op for a title with fewer than two clips or without usable marks —
DVD, HD-DVD and file sources keep the inference path.

## write_frame_at seam/MVC BlockAdditional rationale

`write_frame_at` is `write_frame` (test-only convenience wrapper) with
the frame's SOURCE BYTE OFFSET.

Under a seam plan that offset identifies which clip the frame came from
by lookup rather than by inferring it from the timestamp — which is
ambiguous inside an overlap, where two clips' mark ranges both contain
the same instant. Callers that have a `PesFrame` should pass
`frame.source.map(|s| s.byte)`; `write_frame` is the same call with no
provenance, which falls back to the mark heuristics.

`block_additional`, when `Some`, is attached to the frame as a Matroska
`BlockAdditional` (BlockAddID=2) — Blu-ray 3D (MVC): the base view is the
Block and the dependent (right-eye) access unit rides as the
BlockAdditional under the track's `mvcC` mapping. Such a frame is always
a `BlockGroup` (never a SimpleBlock), with a `ReferenceBlock` when it is
not a keyframe. `None` for every non-3D frame.

## finish() cluster-driver invariant

`MkvMuxer::finish` finishes the MKV file: writes the Cues element.

A cluster only opens on a keyframe from the PRIMARY VIDEO TRACK — the
first track whose type is video, at whatever index it occupies (see
`cluster_driver`, which is `primary_video_track.unwrap_or(0)`; index 0 is
only the fallback for a file with no video track at all). The caller must
deliver a keyframe on that track before (or alongside) other-track data.
This said "track-0" and required the caller to place video at index 0,
which the code has never actually required — so a reader debugging
dropped frames would suspect track ordering, which is not it. If no such
keyframe ever arrives, every `write_frame` is silently dropped; rather
than emit a structurally valid but empty MKV (zero clusters, zero
frames), `finish` returns `Error::MkvInvalid` when frames were submitted
but none were written.

## write_block_group ReferenceBlock/keyframe-bit

`write_block_group` writes a BlockGroup (Block + BlockDuration, plus a
ReferenceBlock when the frame is not a keyframe).

`reference` is `Some(offset_ticks)` for a non-keyframe and `None` for a
keyframe. Inside a BlockGroup the SimpleBlock `0x80` keyframe bit is
reserved and MUST be 0, so a non-keyframe that omits ReferenceBlock is
indistinguishable from an intra frame. This path is NOT subtitle-only:
the MPEG-2 parser stamps a per-frame duration, so every MPEG-2 video
frame (I, P and B) arrives here.

## write_block_group_mvc reference/duration

`write_block_group_mvc` writes a BlockGroup carrying the base view Block
plus the MVC dependent (right-eye) access unit as a `BlockAdditional`
(BlockAddID=2), per the track's `mvcC` BlockAdditionMapping. A
non-keyframe frame gets a `ReferenceBlock` (`reference` = referenced
keyframe offset in ticks) so it is not mistaken for a seek point;
`BlockDuration` is written when known.

## removed parse_resolution/parse_sample_rate/parse_channels history

Old `parse_resolution`/`parse_sample_rate`/`parse_channels` helpers were
removed — `Resolution::pixels()`, `SampleRate::hz()`,
`AudioChannels::count()` replace them.

## keyframe_survives_roundtrip regression history

`keyframe_survives_roundtrip_for_duration_bearing_frames` asserts keyframe
flags survive a write→read round-trip for frames that carry a per-frame
DURATION, i.e. the BlockGroup path.

Regression: every MPEG-2 frame carries a duration (the parser stamps one
on I, P and B alike), so ALL MPEG-2 video is written as BlockGroup, not
SimpleBlock. Inside a BlockGroup the SimpleBlock `0x80` keyframe bit is
reserved and the writer emits 0 — keyframe-ness lives ONLY in the
presence/absence of a ReferenceBlock. Two halves were broken:
  - the writer discarded `keyframe` on this path (no ReferenceBlock ever),
    so a shipped DVD rip marked every P/B frame as a seek point;
  - the reader ignored ReferenceBlock and read the always-0 reserved bit,
    so EVERY BlockGroup frame read back as a non-keyframe.

Downstream that silently dropped all video on `mkv://`(MPEG-2)→`m2ts://`
(TsMuxer drops non-key video until the first keyframe) and made
`mkv://`→`mkv://` / `stdio://` fail E6008 (MkvMuxer opens a cluster only
on a track-0 video keyframe → zero frames written).

A SimpleBlock case (duration `None`) is asserted alongside so a fix that
regresses the non-duration path is caught too.

## mkvstream preroll test

`mkvstream_preserves_video_keyframe_after_audio_preroll` runs end-to-end
through the REAL `MkvStream::create`/`write`/`finish` path (deferred
Pending→activate buffering), in the shape the DVD pipeline actually
produces: several AC-3 audio frames arrive BEFORE the first video frame
(audio emits per-PES immediately, while MPEG-2 holds its first GOP), and
the video frame carries a per-frame DURATION so it is written as a
BlockGroup.

Guards the same regression as
`keyframe_survives_roundtrip_for_duration_bearing_frames`, but across the
buffering machinery rather than the bare muxer — the video keyframe must
still be a keyframe after being buffered and replayed on activation.

## frames_dropped_before_first_cluster_are_counted rationale

Frames dropped before the first cluster opens must be COUNTED, and the
count must survive to `finish()` so it can be reported. The counter was
incremented at two sites and read nowhere — no log, no error, no
accessor — so a partial drop (leading audio/subtitle frames ahead of the
first video IDR, or an M2TS whose PMT lists audio before video) silently
omitted those frames from the output while the run reported
`completed = true` with an empty `undelivered_streams` and nothing in
the log. `finish()` now logs it; this pins the accounting the log
depends on.

## zero_cues_voids_seekhead_entry rationale

When no Cues element is written (zero cue points), the SeekHead must NOT
retain a CUES entry that back-patches to the Cues offset — that offset
now holds Tags / EOF, a dangling pointer to a non-Cues element. finish()
Voids the unused CUES Seek entry instead. (The empty-cues case is
defensive — the normal path pushes a cue with every cluster — so the
test clears the cue list directly before finalizing.)

## second_video_track_pts_not_clobbered rationale

Regression for the second-video-track bug: a Dolby Vision enhancement
layer is video but NOT track 0. The exemption must follow track TYPE, so
the EL's B-frame PTS are preserved exactly like the main video's — not
clamped to prev+1ms (which reintroduced the non-monotonic-DTS flood on
the EL stream). Drives the muxer through both video tracks and asserts
every video block timecode equals its source PTS.

## clip_boundary_with_straggler test rationale

End-to-end output regression (the symptom, at the block-timecode level):
a large clip-boundary reset WITH an interleaved straggler audio frame
from clip 1's tail, driven through the full muxer. Asserts cluster
timestamps are monotonic non-decreasing AND the timeline reaches past
the boundary (clip 2 present) without ratcheting. This is the test that
would have caught BOTH the original `-820000` non-monotonic band and the
straggler ratchet that made everything after the boundary unseekable.

## epoch_driver_follows_primary_video rationale

Regression for the hardcoded-`track 0` epoch driver: on an M2TS/PMT
title the PMT may list an AUDIO ES before the VIDEO ES, so `streams[0]`
is audio and the video is at index 1. The epoch driver must follow the
PRIMARY VIDEO track (first video index), not the literal 0. If audio
(index 0) drove epochs, its sparse/lagging PTS would ratchet the
frontier and false-trigger boundary resets, inflating the timeline. This
drives the muxer with audio=track0 / video=track1 and asserts cluster
timestamps stay monotonic and the timeline does NOT ratchet past the
real span.

## dvd_two_letter_language_becomes_iso_639_2 rationale

RFC 9559 §12 / the Matroska `Language` element spec restrict the legacy
`Language` element to the Matroska language form (ISO 639-2, three
lowercase letters), never ISO 639-1 (two letters). The DVD IFO
audio-attribute block itself carries a raw ISO 639-1 code (e.g. "en") on
disc — `ifo::parse_audio_attr` converts it to ISO 639-2 before returning,
via `ifo::dvd_lang_to_iso639_2`. This mimics the real DVD pipeline
(`disc/dvd.rs`'s `Stream::Audio` construction) end to end: real on-disc
IFO bytes -> `ifo::parse_audio_attr` -> `disc::AudioStream` ->
`MkvTrack::audio` -> the muxer -> the emitted `Language` element.

## a_source_with_no_language_emits_und rationale

A source that knows no language at all leaves `language` EMPTY, and the
muxer writes the `Language` element unconditionally — so an empty string
becomes a zero-length `Language` in the shipped file, which is not a
Matroska language form (RFC 9559 §12 wants three ISO 639-2 letters) and
is not the ISO 639-2 code for "unknown" either.

This is the state every HD-DVD rip is in: `disc::hddvd`'s EVO stream
probe has no language table to read and sets `language: String::new()`
on every audio stream it finds. The DVD path normalises to "und" in
`ifo::parse_audio_attr`; the guard has to exist at the muxer too, which
is the one place every source funnels through.

## dvd_unmapped_or_empty_language_becomes_und rationale

An unmapped or absent DVD language code (bytes 0x00 0x00 in the IFO
attribute block) must degrade to the valid Matroska "undetermined" code
`und`, never to an empty string or a raw 2-letter code — both of which
violate the Matroska language form.

## emitted_language_for_dvd_code

Builds an 8-byte DVD IFO audio-attribute block (AC-3, 48 kHz, 6ch)
carrying `code` in the language bytes, runs it through the real DVD
pipeline (`ifo::parse_audio_attr` -> `disc::AudioStream` ->
`MkvTrack::audio` -> the muxer) and returns the value that actually lands
in the emitted Matroska `Language` element.

## dvd_language_outside_the_menu_vocabulary rationale

The ISO 639-1 -> ISO 639-2 conversion must cover the WHOLE of ISO 639-1,
not just the handful of languages that happen to appear in Blu-ray
menu-graphic filenames. A Region-2 disc routinely carries Romanian,
Bulgarian, Croatian, Serbian, Slovak, Slovenian, Hebrew, Estonian,
Latvian, Lithuanian, Icelandic and so on; if those all collapse to `und`,
every one of a disc's subtitle tracks emits the same `Language` value and
nothing else tells them apart (DVD streams carry an empty `label`). A
valid-but-identical code is worse for the user than the invalid one it
replaced, so each of these must reach the emitted `Language` element as
its own correct three-letter code.

## dvd_era_language_aliases_map_to_the_modern_code rationale

DVD-Video froze its language list on the 1988 edition of ISO 639-1,
which spelled Hebrew `iw`, Indonesian `in` and Yiddish `ji`. Real discs
authored to that list carry those bytes, so they must map to the same
ISO 639-2 codes as the modern `he` / `id` / `yi` spellings rather than
degrading to `und`.

## unknown_dvd_language_still_yields_exactly_und rationale

Widening the table must not weaken the degradation guarantee: a code
that is not ISO 639-1 at all still has to yield exactly `und`, a valid
Matroska language value, and never a passed-through two-letter code, an
empty string, or a guess.

## mkv_pgs_wrong_forced_label_is_cleared rationale

A wrong vendor forced label is CLEARED by the content — but only where
the content can carry that argument. During a full mux every display set
on the track is seen, so "this track has hundreds of display sets and
not one of them is forced" is as complete as evidence gets; and a
sibling track that does carry `forced_on_flag` proves the authoring
house sets it, so the absence on this track means something.

Before this, `finish()` only ever promoted 0→1, so a track wrongly
labelled forced stayed forced in the output no matter what the disc
contained.

## mkv_pgs_forced_label_survives rationale

The guard that stops the above clearing from being reckless. On a disc
whose authoring never sets `forced_on_flag` — they exist — no track has
any forced display set, so "no forced display set here" is a fact about
the authoring, not about the track. The vendor label is then the only
information there is and must survive.

## a_title_the_seam_plan_emptied audit finding

`a_title_the_seam_plan_emptied_is_not_reported_as_a_skippable_stub`
asserts a seam-plan clip list whose marks exclude every frame must fail
with `SinkWroteNothing`, NOT `MkvInvalid`.

Audit finding: both gates in `finish()` were dead code under test —
`set_clips` was never called anywhere in the suite, so
`continuity.dropped_total()` was always 0 and neither branch could be
reached. Swapping the two `frame_count == 0` checks left the whole suite
green, while a title the seam plan emptied came back as `MkvInvalid` —
which `is_skippable_title_stub` classifies as an empty nav/menu stub, so
an all-titles rip would silently omit a real feature and exit 0.

## all_block_groups rationale

`all_block_groups` parses EVERY BlockGroup in the output, in emission
order.

A presence check like `find_id(data, ebml::REFERENCE_BLOCK).is_some()`
cannot express what actually matters here: ReferenceBlock's ID is the
single byte 0xFB, and finding one somewhere in the file says nothing
about WHICH blocks carry it. A mutant emitting a ReferenceBlock on every
BlockGroup — keyframes included — satisfies that check while making
every frame read back as a non-keyframe, which is the exact defect these
tests exist to guard.

## dolby_vision_config dvcC bit-packing layout

`dolby_vision_config` (dvcC) bit packing: byte 2 = profile(7 bits) << 1 |
level high bit; byte 3 = level low 5 bits << 3 | rpu | el | bl; byte 4 =
bl_compat_id << 4.

## parse_bps_tags decode rationale

`parse_bps_tags` parses the `Tags` master into `(TagTrackUID, BPS)`
pairs, decoding the BPS `TagString` back to a number.

The previous assertion was
`String::from_utf8_lossy(&data).contains("800")` over the WHOLE file,
which a wrong BPS passes trivially — 80000 contains "800", and so does
any unrelated byte run that happens to spell it. The bitrate a media
player displays has to be the real one, so decode it.

## seekhead_chapters_entry rationale

The Chapters Seek entry has to be back-patched like every other one. It
is the only fixup whose offset comes from an `Option`, so it is the only
one that can quietly fall through to the `_ => 0` arm and ship a
SeekPosition of 0 — a chapter index a player resolves to the Segment
header. Only titles WITH chapters exercise it, and every SeekHead test
until now used a chapterless title.

## video_track_survives_a_zero rationale

`MkvTrack::video` divides by two disc-supplied numbers: the frame-rate
numerator and the display-aspect denominator. Both come off an IFO/MPLS
scan of a damaged disc, and a zero in either one is an arithmetic panic
that takes down the whole `autorip` service — not a bad file. The guards
exist; nothing proved they were load-bearing.

## secondary_streams_are_never_the_default rationale

FlagDefault marks the track a player selects with no user input. A
SECONDARY stream (a Dolby Vision enhancement layer, a director's
commentary) must never be it: default-selecting the DV EL shows a viewer
the wrong picture, and default-selecting a commentary track the wrong
audio. `is_default` is the inverse of `secondary` on both the video and
the audio builder; neither inversion was asserted.

## subtitle_codec_id_distinguishes_vobsub_from_pgs rationale

DVD subtitles are VobSub, Blu-ray subtitles are PGS, and the codec ID is
what tells a player which parser to hand the bitstream to. The subtitle
builder's fallback is PGS, so a lost `Codec::DvdSub` arm silently labels
every DVD subtitle track as PGS — a track that displays nothing.

## hdr_format_overrides_the_transfer rationale

The HDR transfer override is the only thing that turns the coarse
`color_space` nibble into a PQ/HLG code point. Every earlier test paired
HDR10 with BT.2020, whose enum mapping already yields PQ — so the
override itself was never observed. A disc whose playlist nibble says
BT.709 while the scan detected HDR10 (an ordinary UHD mis-tag) is the
case that separates them: without the override it ships transfer 1 (SDR
gamma) and the picture renders washed out.

## mvc_decoder_config_record_boundaries rationale

`mvc_decoder_config_record` frames a Blu-ray 3D parameter set with
16-bit length prefixes. Its bounds are exact: 4 bytes is the shortest
subset SPS it can read `profile`/`compat`/`level` out of, and 0xFFFF is
the largest length the field can express. Off by one at either end and a
valid 3D title silently loses its mvcC mapping (no 3D signal) or writes
a record whose declared length does not match its payload.
