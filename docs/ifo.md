# IFO parser notes

Overflow rationale relocated from `src/ifo.rs` comments (see `// See docs/ifo.md`
pointers in that file for the anchor point of each section below).

## `DvdTitle::feature_start_cell` (Bug-4: scene-selection/logo at head of feature)

The main feature's PGC can open with leading cells that are NOT part of the
movie — a scene-index segment or an interleaved-angle sub-block. This walks
the leading run and returns the index of the first cell that is a plain
feature cell (category `0x00`-class).

Conservative by construction — it can NEVER truncate a normal feature:
- It only ever skips a *prefix*; the scan stops at the first plain-feature
  cell and keeps everything from there on.
- A normal single-angle feature has category `0x00` on cell 0, so the scan
  stops immediately at index 0 and drops nothing.
- It never drops on duration or any heuristic — only on the spec category
  bits — and it never drops the FIRST cell of an angle block (the angle we
  keep).
- As a final guard it never returns past the last cell, and never drops when
  that would leave zero cells.

For a real disc where every feature cell category is `0x00` and chapter 1
sits at 00:00:00, this returns 0 — a no-op — which is the correct result: the
disc's scene-index lives in a separate menu/title PGC, not in leading cells
of the feature PGC, so there is nothing to drop here.

## `bcd_to_frames` / `bcd_to_secs` (dvd_time_t is a timecode, not wall-clock time)

Format: `[hours_bcd, minutes_bcd, seconds_bcd, rate_and_frames]`
  - Byte 0: hours in BCD (e.g. 0x01 = 1 hour, 0x12 = 12 hours)
  - Byte 1: minutes in BCD
  - Byte 2: seconds in BCD
  - Byte 3: bits 7-6 = frame rate flag (01=25fps, 11=29.97fps),
    bits 5-0 = frame count in BCD

The seconds field advances once every [`DvdRate::nominal_fps`] frames. On
NTSC discs that is every 30 frames, but 30 frames of 30000/1001 fps video
last 1001/1000 s — so reading H:M:S as literal seconds under-reports real
time by exactly 0.1% (3.6 s per hour). Callers that need real seconds must go
through the frame count, which is what `bcd_to_secs` does.

## `MAX_TT_SRPT_TITLES` / `parse_tt_srpt`

DVD-Video caps a disc at 99 titles (VMGI TT_SRPT `TT_Ns`, and the 99-title /
99-title-set structure the format is built around), but the on-disc count is
an untrusted `u16`: a ~800 KB crafted IFO can declare 65535 entries, each
re-parsing a PGC into a full `DvdTitle` (~540 MB of `DvdInfo`). Headroom: this
IS the format maximum, so it clips no conformant disc — a real DVD cannot
address a 100th title through TT_SRPT.

`parse_tt_srpt` has two bounds on untrusted input: the declared entry count
is clamped to `MAX_TT_SRPT_TITLES`, and entries naming a `(vts_number,
vts_title_num)` pair already seen are dropped. De-duplication is a
correctness fix as well as a bound: two TT_SRPT entries pointing at the same
VTS title are the same title, and `parse_pgcit` would otherwise re-parse
that one PGC into a separate `DvdTitle` per entry.

## `parse_audio_attr` visibility

`pub(crate)` for the CROSS-MODULE tests only. The sole production caller is
`parse_vts_attributes` in this file; `src/mux/mkv.rs`'s `#[cfg(test)]` block
calls it directly so its language-mapping tests run the real parser over real
on-disc IFO bytes end to end, instead of a hand-built `DvdAudioAttr` that
could agree with the muxer while both disagree with the disc. Narrowing this
would mean either a `#[cfg(test)]`/`#[cfg(not(test))]` pair of signatures
that can drift apart, or moving those tests away from the code they exist to
pin — both worse than the widened crate-internal visibility, which reaches no
public API.

## `assign_audio_sub_stream_ids`

On DVD-Video the sub-id's low nibble is the audio-stream *number* (0-7),
shared across all codecs — the single stream index the PGC `audio_control`
table / navigation registers select — and the high nibble is the codec base.
So the sub-id is `codec_base | position`, where `position` is the stream's
index in the IFO audio-attribute table (NOT a per-codec running count):
  - AC-3  → `0x80 | i`
  - DTS   → `0x88 | i`
  - LPCM  → `0xA0 | i`
  - MP1/MP2 and anything else → `None` (regular MPEG-audio PES, not a
    private-stream-1 sub-id).

A per-codec ordinal was wrong: it only coincides with the wire id when a
codec's first stream is also the disc's audio stream #0. Any codec that is
not the first audio stream (e.g. a DTS track after an AC-3 track) then got
a sub-id one-too-low, so the demux routing key (`0xBD00 | sub_id`) never
matched and the track muxed silent. The positional index is the real wire
number, so distinct positions still give distinct sub-ids (no collision).

Position saturates at 7 so a malformed over-count never produces an
out-of-range sub-id.

## `parse_raw_dvd_lang_bytes`

The salvage is deliberately not narrowed to a-z: whatever survives is only
ever a lookup key for `dvd_lang_to_iso639_2`, which degrades anything it does
not recognize to `und`, so a stray `X` or `5` costs nothing and cannot reach
an output stream as a language code.

## `dvd_lang_to_iso639_2`

Uses `labels::vocab::iso639_1_to_iso639_2`, which spans the WHOLE of ISO
639-1 (plus the withdrawn spellings `iw`/`in`/`ji` that DVD-Video's
frozen-1988 language list still puts on disc). The narrower `vocab::menu_lang`
table is deliberately NOT used here: it exists for Blu-ray menu-graphic
filename tokens and knows only 25 languages, so a Region-2 disc's Romanian,
Bulgarian, Croatian, Serbian, Slovak, Slovenian, Hebrew, Estonian, Latvian,
Lithuanian and Icelandic tracks would all fold onto `und` together. DVD
streams carry an empty `label`, so the language is the only thing
distinguishing one subtitle track from the next — a valid code that is
identical for six tracks is worse for the user than the invalid one it
replaced. Both tables normalize to ISO 639-2/T, so they agree wherever they
overlap.

An empty or unrecognized code degrades to `"und"` (ISO 639-2 / Matroska's own
"undetermined" value) — a valid element value — rather than passing through
an invalid 2-letter code or an empty string. Never guesses.

## `video_attr_absolute_bytes_pin_real_layout` test

ABSOLUTE-BYTE pin (audit §3 #2): the existing video-attr tests build the byte
via `v_atr_byte(...)`, which uses the SAME shift constants the parser reads
with — a co-edit of constant + helper would silently re-introduce the
PAL-as-NTSC bug and every test would still pass. This test feeds
`parse_video_attr` HARDCODED bytes captured from real DVD-Video layouts
(DVD-Video video attributes: mpeg_version[7-6] video_format[5-4]
display_aspect[3-2] permitted_df[1-0]) — no `v_atr_byte`. If the parser's bit
positions drift, these fail.

## `dts_after_ac3_uses_positional_substream_id` test

Regression (The Punisher 2004): audio[0]=AC-3 5.1, audio[1]=DTS 5.0. The DTS
track sits at audio position 1, so its wire sub-id is 0x89 (0x88 | 1), NOT
the per-codec 0x88. With the old per-codec ordinal it got 0x88 → demux
routing key 0xBD88 had no match → every DTS packet (which carries 0x89) was
dropped → the track muxed present-but-silent while the AC-3 (at position 0,
where ordinal and position coincide) played fine. Positional numbering fixes
it end-to-end.

## `pgc_chapter_times_ntsc_no_pulldown_drift` test

Regression test for issue freemkv#25 (NTSC chapter drift). Builds a PGC from
the real cell table of the disc in the bug report and checks the chapter
marks against the reference tool's values. Before the fix, chapter 14 landed
4.019 s early; the drift was proportional to elapsed time (0.1%), so a short
synthetic fixture would not have caught it.

## Test additions note

The block of tests starting at `cell_category_low_flags_are_bit_isolated`
covers cell-category bit isolation, palette/program-map arithmetic, and the
malformed-language salvage paths.

## `build_pgc` test fixture

Builds a standalone PGC starting at offset 0. Layout: 0xEA-byte header, then
the program map (one byte per program, the 1-based first cell number), then
the 24-byte cell playback table. `cells` are `(category, BCD time,
first_sector, last_sector)`.

## `build_pgcit` test fixture

Builds a VTS_PGCIT at offset 0: VTS_PGC_Ns(2) + reserved(2) +
VTS_PGCIT_EA(4), then one 8-byte VTS_PGCI_SRP per PGC (VTS_PGC_CAT(4) +
VTS_PGCI_SA(4), the PGC's byte offset from the VTS_PGCIT start). `pgcs` are
appended after the SRP table.
