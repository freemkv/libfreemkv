# `src/labels/mpls_universal.rs`

Universal MPLS-based stream labels — the confidence-tier "floor" for the
`labels` registry.

## Why this module exists

Unlike the framework-specific parsers in `src/labels/` (dbp, pixelogic, ctrm,
criterion, ...), this module is the floor: every Blu-ray ships with MPLS
playlists under `/BDMV/PLAYLIST/`, and every MPLS file has an STN table with
per-stream ISO 639-2 language codes plus coding-type / channel-layout /
sample-rate bytes from the BD spec.

The framework parsers extract richer editorial labels ("English Dolby Atmos",
"Director's Commentary") when the disc was authored with a recognized tool.
When none of them match (e.g. a "no BD-J" disc, or an authoring framework we
haven't catalogued), MPLS still gives us language + codec on every stream —
enough to render something more useful than the bare PID.

Output confidence is Low: MPLS carries language + codec but never
purpose/qualifier info (no way to tell "Commentary" from "Normal" from the STN
table alone). Higher-confidence framework parsers, when present, always win on
the registry's max-by-confidence tiebreaker — MPLS is only chosen when nothing
else matched.

## `build_labels`

Converts every stream entry across `playlists` into one `StreamLabel` per
physical stream. Factored out of `parse` so unit tests can drive the actual
conversion logic (stream-type mapping, identity, slot numbering) directly from
already-parsed `crate::mpls::Playlist` values, without needing a synthetic
on-disc UDF image.

Identity is `(clip, PID)` — what the STN entry states — and it is both the
dedup key and the label's `StreamId`. A stream twenty playlists list is one
label; two clips that both open their first audio at 0x1100 are two. This
replaced a disc-global dense counter that numbered surviving entries 1, 2, 3,
… in playlist-directory order: that number was not an STN slot in anything,
but it was handed to a binder that reads `stream_number` as one.

## `label_type_for`

Which per-type numbering list an STN entry belongs to, or `None` when it is
not a labellable stream at all.

This MUST agree with the stream list `disc::bluray` builds from the same
entries, because that list is what `labels::apply_labels` counts against when
it binds `stream_number`. The two counters run over the same STN entries in
the same order, so any entry one side keeps and the other drops — or files
under a different type — shifts every later label of that type onto the wrong
stream. Three rules, all mirroring `disc::bluray`:

* `coding_type == 0` is the STN table's empty/padding slot. Not a stream on
  either side.
* a PG coding_type in an audio STN slot is a subtitle, not audio.
  `mpls::parse_stream_entry` has a dedicated arm for this layout, so it is an
  authored shape rather than a corruption.
* video (1 / 6 / 7 = primary, secondary, Dolby Vision EL) and IG (4) have no
  `StreamLabelType`; they are numbered in their own STN lists and never
  interleave with the audio or PG lists.

## Test helpers and fixtures

`playlist_with`: a playlist over clip "00001". `Playlist::streams` is read out
of the first play item's STN table, so a playlist that has streams always has
a play item to have read them from — the fixture carries one so tests exercise
the shape production sees, and so each label gets the `(clip, PID)` identity
it is bound by.

`labels_from_playlists`: drives the actual production conversion logic
(`build_labels`, the function `parse()` calls) starting from already-parsed
Playlists, so tests don't have to synthesize valid on-disc MPLS/UDF bytes.
This calls the *real* code under test rather than a hand-written
re-implementation, so mutations inside `build_labels` (stream-type mapping,
dedup key, counters) are actually caught here.

`padding_stn_entry_does_not_consume_a_label_slot`: `stream_number` is bound by
`labels::apply_labels` against the title's own stream list, which
`disc::bluray` builds from these same STN entries. That builder DROPS an entry
whose `coding_type` is 0 — the STN table's empty/padding slot — so it must not
be counted here either. Counting it advances the audio counter past a stream
that never materializes, and every label behind it binds one stream late.

`pg_coding_type_in_an_audio_slot_counts_as_a_subtitle`: a PG coding_type
sitting in an audio STN slot is a real, documented shape —
`mpls::parse_stream_entry` has an explicit arm for it, and `disc::bluray`
builds it as a Subtitle stream, not an Audio one. This module must classify it
the same way, or the audio counter runs one ahead and the subtitle counter one
behind for every later stream.

`same_pid_in_two_clips_is_two_streams`: the same PID in two DIFFERENT clips is
two different streams — a PID is only unique within one clip. Deduping on the
PID alone (as the old key's `(type, language, codec_hint, pid)` did across
clips) collapses them into one label, and the second clip's stream is then
described by the first clip's.
