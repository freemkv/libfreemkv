# Pixelogic label parser — internal rationale

Long-form notes for `src/labels/pixelogic.rs` internal (non-pub) items, kept
here so the source comments stay within the comment-guard's internal cap.
Each source site links back with `// See docs/pixelogic.md — <topic>`.

## MAX_STREAMS_PER_TYPE

Sane upper bound on streams of one type within a single feature section. The
BD STN table caps audio at 32; this generous ceiling stops a crafted blob
with tens of thousands of stream tokens from overflowing the u16 STN
counters (panic in debug, wrap-to-0 in release, which would misnumber
subsequent labels).

## MAX_VIDEO_SLOTS

Sane upper bound on the number of DISTINCT video-slot entries one section may
list before the walk gives up on it. A section's stream list opens with its
video slots, and `assign_labels` remembers them to recognise where the NEXT
section starts (see the loop body). The BD STN table admits one primary
video plus at most 32 secondary ones, so a section claiming more than that
is not a stream list — and the memo must not grow without bound on disc
bytes.

## MAX_REPORTED_UNKNOWN

How many DISTINCT uncatalogued token components one parse will retain for
the end-of-parse report. Disc bytes are untrusted, so the set that backs the
report is capped: a crafted blob carrying thousands of distinct components
must not grow it without bound. Past the cap the components are still
counted (and still logged individually at debug), just not retained by
name — the report says so.

## UnknownParts

Collects the uncatalogued token components one parse ran into, so the run
can report them ONCE at the end instead of either staying silent or
emitting a line per occurrence.

Why aggregate: an unmapped vendor component is how a forced/SDH/commentary
qualifier goes missing, and a per-occurrence `debug!` is invisible in
practice — the gap only surfaces when a user complains about a mislabelled
track. But a per-occurrence `warn!` is unusable in the other direction: a
disc can carry dozens of per-language segment names that merely COLLIDE
with the `{lang3}_{component}` token shape (localized notice/disclaimer clip
names, for instance), and warning on each would bury real signal under
routine noise. One bounded, deduplicated line per parse is loud enough to
notice and quiet enough to live with.

`seen`: distinct components, deduplicated and ordered for a stable log line,
bounded by `MAX_REPORTED_UNKNOWN`. `total`: occurrences including ones past
the retention cap.

## assign_labels

Walk the extracted token strings of the feature section and emit a
`StreamLabel` per editorial token, numbered in STN order. Split out from
`parse` so the section/numbering logic is unit-testable without a
`SectorSource`/`UdfFs`.

## placeholder_kind

The bare `Audio Stream N` / `PG Stream N` slot placeholders pixelogic emits
for a stream with no editorial label, and which list they belong to. `None`
for anything else (including the section's `AR_…` aspect-ratio entry, which
is not part of either numbered list; the `Video Stream N` entries are
consumed by the section-boundary rule in `assign_labels` before they get
here, and belong to neither list either).

## is_stream_token

Whether a string has the shape of a pixelogic stream token —
`{lang3}_{component}…` — regardless of whether its components are
catalogued. The gate is exactly the one `parse_token_inner` applies before
it starts classifying, so every token that parser could ever accept is
recognised here as occupying a stream slot, and nothing else is.

## Test: a_full_subtitle_token_is_never_forced_without_its_own_forced_component

Immunity pin against the defect measured in the `paramount` parser: a vendor
"forced" marker that sits on a FULL dialogue track's own slot to mean "this
track also contains forced signs", read as "this track is forced" and so
flagging full dialogue tracks forced.

This grammar cannot express that. The forced marker is a component of a
slot's OWN token, so a forced-narrative pass occupies a slot of its own
(`{lang}_TXT_FOR_`, `{lang}_DUB_`) alongside the language's separate
full-dialogue slot — it is never a parallel array indexed against the full
tracks' slots, which is the shape that let one vendor's marker land on a
dialogue track.

Mutation: give any full-dialogue component (`SDLG`, `TXT`, `SDH`, `STRI`,
`SCOM`) a forced qualifier of its own.

## Test: assign_labels_numbers_subtitles_by_stn_slot_not_by_parsed_token

Taken from a real UHD feature's `SEG_MainFeature`: the PG list has 18 slots,
five of them bare `PG Stream N` placeholders and four more carrying a token
whose only non-language component is a REGION (`fra_CF_`, `spa_LS_`, …) —
which `parse_token_inner` rejects because it signals neither audio nor
subtitle. Every one of those still OCCUPIES an STN slot, so the run of
forced-narrative tokens sits at STN 11-18. Numbering only the tokens that
parse collapsed them onto STN 2-8 — the disc's FULL subtitle tracks — so the
player offered "English (forced)" that renders the whole English dialogue.

The run is also where the vocabulary half of the same bug shows: the slot at
STN 17 spells its forced-narrative marker `DUB` rather than `TXT_FOR`, and
until `DUB` was catalogued that one track alone stayed unflagged even once
the numbering was right.

## Test: assign_labels_fpl_section_ends_on_sf_boundary

The FPL section also ends on an `SF_` marker (not just `SEG_`/`FPL_`). Only
`assign_labels_fpl_section_ends_on_seg_boundary` existed before, which
cannot distinguish a mutated `||` chain from the correct one (any single
true operand already ends the section). This test isolates the `SF_`
alternative specifically. Mutation: `||` -> `&&` in the end-of-section check
would require ALL THREE prefixes to match simultaneously (impossible for a
real single token), so the section would never end on `SF_` alone.

## Test: assign_labels_section_ends_at_the_next_sections_video_slot

The feature section also ends where the NEXT section's stream list starts,
which is the only boundary available when the feature playlist is the last
NAMED (`SEG_`/`SF_`/`FPL_`) section in the blob.

Shape taken from a corpus disc whose feature playlist is the last named
section: the trailing per-language notice/disclaimer cards are emitted as
unnamed sections, each opening with its own `Video Stream 1` / `AR_…` pair
and titled with a plain clip name. Those clip names are `{lang3}_{card}`, so
they pass the stream-token gate and each one advances an STN counter; a
card whose name collides with a catalogued component (`AC` reads as the
AC-3 codec) even emits a label, for an STN slot the feature playlist does
not have. On that disc the walk ran 95 entries past the end of the
feature's own list, fabricated five audio labels at STN 10-14 (the playlist
has 9 audio slots), and reported 94 uncatalogued components — which also
downgraded the whole parse from High to Medium confidence.

Mutation: drop the repeated-video-slot boundary → `deu_Warning` and
`fra_ND` advance the subtitle counter and `eng_AC` emits a phantom Dolby
Digital label on an audio slot that does not exist.

## Test: assign_labels_video_slot_memo_is_bounded

The memo of a section's video slots is built from disc bytes, so it is
bounded: past `MAX_VIDEO_SLOTS` distinct entries the section is not a
stream list and the walk stops instead of retaining them all. Mutation:
drop the length guard → the memo grows with the blob.

## Test: assign_labels_audio_cap_alone_does_not_stop_subtitle_processing

The two per-type caps are independent — the loop only stops early once
BOTH audio and subtitle counters have reached `MAX_STREAMS_PER_TYPE`.
Reaching the audio cap alone must not cut off subtitle processing.
Mutation: `&&` -> `||` in the outer stop-condition would break the loop as
soon as EITHER counter reaches the cap, silently dropping a legitimate
subtitle stream that comes after audio saturates.

## Test: assign_labels_subtitle_cap_alone_does_not_stop_audio_processing

Companion to the above: with the subtitle counter saturated but audio
still under its cap, a subsequent audio token must still be processed.
Isolates the first `>=` operand (`audio_num >= MAX_STREAMS_PER_TYPE`) from
the second. Mutation: `audio_num >= MAX_STREAMS_PER_TYPE` -> `audio_num <
MAX_STREAMS_PER_TYPE` would flip the stop-condition to trigger whenever
audio is UNDER cap and subtitle is AT/over cap — exactly this scenario —
dropping the trailing audio token.

## Test: assign_labels_pg_placeholder_advances_sub_counter_only

A `PG Stream N` placeholder occupies a PG STN slot, so it advances the
subtitle counter exactly as `Audio Stream N` advances the audio one, and
the two counters stay independent. Mutation: skip PG placeholders → every
later subtitle label shifts down by the number of unlabelled PG slots
ahead of it.

## Test: assign_labels_unclassifiable_token_still_occupies_a_slot

A token-shaped entry the grammar cannot classify (`fra_CF_` — REGION only,
so it signals neither audio nor subtitle; `jpn_ZZQ_` — an uncatalogued
component) still occupies an STN slot in the list currently being
enumerated. Mutation: skip unclassifiable tokens → later labels shift down.

## Test: assign_labels_dub_slot_is_forced_in_a_forced_run

A `DUB` slot sitting inside a run of `*_TXT_FOR_` siblings — the shape both
corpus discs show — yields a forced label on ITS OWN slot, contiguous with
its neighbours. Mutation: classify DUB as audio → the PG run gains a hole
at that slot and the audio list gains a spurious entry.

## Test: unknown_parts_collapses_a_wall_of_segment_name_collisions

The per-language notice/disclaimer clip names some discs carry (`{lang}_ND`,
`{lang}_Warning`, …) merely COLLIDE with the token shape. They are not
stream tokens and carry no editorial meaning, so they must stay
uncatalogued — mapping them would attach a qualifier to a stream on the
strength of a filename. What they must do is collapse into ONE report
rather than one line each. Mutation: warn per occurrence → dozens of lines
on an ordinary disc.
