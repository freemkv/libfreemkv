# Paramount/onQ `playlists.xml` — derivation notes

`/BDMV/JAR/` is application-defined space, so this format has no spec to
look up. Every field meaning below was derived by measuring real discs and
cross-checking against per-display-set content. Treat an unfamiliar value
as unknown rather than guessing — the disc's own `forced_on_flag` is the
only authoritative forced signal.

## `ForcedSub` — the `forced_sub` CSV cell values

The attribute reads like a boolean and was once parsed as one (`cell ==
"1"` → forced). It is not. Every image in the corpus carrying this
vendor's `playlists.xml` — seven distinct discs — uses four values, and
decoding three of those discs' feature subtitle tracks and counting every
PGS display set separates them into two populations two orders of
magnitude apart:

* `0` — a subtitle track with no forced-narrative content. On the two
  discs measured that use the flag at all, not one `0` track carried a
  single `forced_on_flag` display set.
* `1` — a FULL DIALOGUE track that additionally contains some
  forced-narrative signs. On one measured disc, all nine `1` cells are
  full tracks of 949-1411 display sets, eight of them carrying 5-14
  flagged sets and the ninth none; that disc has no dedicated forced
  track at all. On another, all seven `1` cells are full tracks of
  1602-1651 display sets carrying 0-31 flagged sets. Reading `1` as
  forced is what made one language present as two identical full
  subtitle tracks with one of them flagged forced.
* `2` and `3` — a DEDICATED forced-narrative track. These take their own
  trailing STN slots, one per localized language, duplicating a language
  that already holds a full track earlier in the list. Measured: the two
  `2` slots on one disc are 15 and 10 display sets, EVERY one flagged
  forced, against ~1600 on that disc's full tracks; the four `3` slots on
  another are 7, 14, 23 and 59 display sets against 1216-2655. What
  distinguishes `2` from `3` the corpus does not reveal — both sit in the
  same trailing position, both measure the same shape, and one disc uses
  each for a different language — so both map alike.

So the old reading was wrong in BOTH directions: it flagged full dialogue
tracks forced, and it discarded the cells that name the real forced
tracks.

The `1` case is deliberately NOT carried through as a weaker "contains
forced segments" hint. There is no qualifier for that, and the asymmetry
argues against inventing one here: a wrong forced flag on a 30 MB
dialogue track is the user-visible defect, while a missing hint costs
nothing.

Nothing downstream can undo a wrong forced label on the discs that need
it most: `mux::codec::pgs::demotable` may only clear a vendor forced
label where some track on the disc demonstrably sets `forced_on_flag`,
and measured discs using this label format never set it.

## `MAX_COM_INDICES` — the CSV addressability bound

The highest CSV cell position that can ever be addressed. The labelling
loops number cells 1-based into a `u16` and `break` at
`u16::try_from(i + 1)`, so cell `MAX_COM_INDICES` and everything past it
is never visited. Two different things are measured against that bound,
and they are not the same:

* A VALUE at or beyond it cannot match any cell. This caps the set:
  values are filtered before insertion, so at most `MAX_COM_INDICES`
  distinct entries can ever be stored, however long the attribute is.
  The `HashSet` that replaced a linear scan fixed the LOOKUP cost; this
  fixes the ALLOCATION, so a disc declaring half a billion indices no
  longer costs half a billion entries.
* A POSITION at or beyond it describes nothing new. This caps the WORK,
  not the memory — the value filter already made the set small, but
  without it every one of those half a billion cells is still split and
  parsed. It is an early exit at the first position whose contents
  provably cannot matter, and it is why `forced_sub` — which holds no
  set at all, and so gets no protection from the value rule — is bounded
  too.

Real authoring is nowhere near either limit: the BD STN table admits at
most 32 streams per playlist, so nothing legitimate is lost.

## `com_indices` / `forced_subs` — why they are separate functions

Extracted so each bound is independently observable in a unit test.
Asserting the bound through `labels_from_feature` is not possible in
either case: for `com_indices`, a `HashSet` collapses repeated values and
an out-of-range index changes no label either way, so a label-level test
passes whether or not the cap exists. For `forced_subs`, the Vec is read
only as `forced.get(i)` from a loop that stops at `MAX_COM_INDICES`, so
cells past that are unreachable by construction and, again, unobservable
through the labels. Returning the raw set/Vec lets a test hand in tens of
thousands of distinct unaddressable entries and see them refused.

## `labels_from_feature`

Split out from `parse` so the per-type numbering and commentary/forced-
index logic is unit-testable without a `SectorSource`/`UdfFs`.

## Test: `a_playlists_stream_list_cannot_run_into_the_next_playlist`

Immunity pin, section-boundary half. The pixelogic parser walks a flat
string sequence and recognises its feature section's END by marker
alone, so a section with no marker behind it runs off into whatever
follows and counts it as more STN slots. Nothing here can do that: the
stream list is one attribute of one XML element, so its length is the
CSV's own cell count and its scope is the element's byte range that
`xml::find_element` returns. Text after the element — including the next
playlist's own `aud` — is not reachable from it.

And when the boundary is MISSING the failure is closed, not open:
`xml::find_element` needs a matching close tag and yields `None` without
one, so an unterminated element ends the walk rather than swallowing the
rest of the document.

Mutation: hand `labels_from_feature` the document instead of the
element, or let an unterminated element run to EOF → the bonus
playlist's languages join the feature's stream list.

## Test: `bounding_the_parse_does_not_change_a_legitimate_playlist`

`sub_com1_idx` is parsed straight out of the disc's `playlists.xml`,
which is attacker-controlled and has no length bound of its own.

This replaces a WALL-CLOCK test. That one built a 200 000 x 1 000 001
fixture and failed if it took over 10 s, to prove the membership test
was a set rather than a linear scan. Measured on the machine that wrote
this: 1.62 s alone, and OVER 10 s — a real failure — when the suite's
other 3 347 tests were running concurrently. A 6x margin against a
shared CPU is not a margin; it is a CI failure that looks like a flake
and gets re-run until it passes.

It also measured the wrong thing. Making the lookup O(1) bounded the
QUERY, not the PARSE: the set was still built from every entry the disc
declared, so a hostile playlist could still force an unbounded
allocation before any lookup happened. `MAX_COM_INDICES` bounds that.

What this test guards is that bounding did not change what a legitimate
playlist MEANS: it goes red if the bound is set too LOW (verified at 2 —
the real indices `0,2,4` stop resolving and the purposes change). It
does NOT go red if the bound is deleted entirely, because the
out-of-range filler is unobservable at the label level and a `HashSet`
collapses the repeats. Enforcement is proven separately, by
`distinct_unaddressable_indices_are_refused_not_stored`, which reads the
set itself. Two tests, two properties; neither pretends to the other's
job.

## Test: `distinct_unaddressable_indices_are_refused_not_stored`

The set REFUSES unaddressable indices, so a hostile playlist cannot
inflate it. DISTINCT values on purpose: a `HashSet` collapses repeats,
so a million copies of one index costs one entry and would prove
nothing. Fifty thousand distinct out-of-range indices cost fifty
thousand entries without the filter, and none with it — so this test
goes red if the bound is removed, which the label-level assertions
elsewhere cannot do.

## Test: `forced_sub_cells_past_the_last_addressable_one_are_not_parsed`

`forced_sub` is bounded too — the third CSV in the same function, and
the one that had no value filter to hide behind. Read through
`forced_subs` rather than through the labels for the same reason the two
tests above read the set: the subtitle loop stops at `MAX_COM_INDICES`,
so a label-level assertion cannot tell a bounded parse from an unbounded
one.

## Test: `an_index_that_cannot_address_any_cell_is_not_retained`

Asserted through `com_indices`, not through the labels: the labelling
loop never queries a cell position that high, so at the label level
retaining the value is unobservable and the assertion could not fail.
Reading the set is what makes the claim checkable.

## Test: `empty_csv_slot_still_occupies_its_stn_slot`

The `aud` / `sub` CSVs are the vendor's STN-ordered stream lists: one
slot per stream, and `aud_com1_idx` / `forced_sub` are indexed against
those same slot positions. A slot whose language cell is empty carries
nothing to label but still OCCUPIES its slot, so it must not renumber
the slots behind it.

Numbering only the slots that carry a language collapsed every later
label one position forward per empty cell, which is how a forced marker
authored for one STN slot lands on the full-subtitle track in front of
it.

## Test: `a_contains_forced_segments_cell_is_not_a_forced_track`

`forced_sub` is an enumeration, and `1` is its "full dialogue track that
also carries forced signs" value — NOT "this track is forced". Measured
on a disc whose feature declares nine `1` cells among 32 subtitle slots:
all nine are full dialogue tracks of 949-1411 display sets, and the disc
has no dedicated forced track at all. Reading `1` as forced is what
produced two identical full subtitle tracks for one language with one of
them flagged forced. See "`ForcedSub` — the `forced_sub` CSV cell
values" above for the full corpus measurement, including why nothing
downstream can undo this.

## Test: `a_dedicated_forced_narrative_cell_is_a_forced_track`

`2` and `3` are the cells that DO name a dedicated forced-narrative
track, and the old boolean reading discarded both. Measured: these cells
occupy their own trailing STN slots, one per localized language,
duplicating a language that already holds a full track earlier in the
list. On one measured disc the four `3` slots carry 7, 14, 23 and 59
display sets against 1216-2655 on the full tracks they duplicate — and
not one display set anywhere on that disc carries `forced_on_flag`, so
neither the scan probe nor the muxer can promote them from content. The
vendor cell is the only evidence there is.

## Test: `an_unrecognised_forced_sub_cell_is_not_forced`

An unrecognised cell must fall to NOT forced. Asserting forced is the
expensive mistake (a full dialogue track a player then burns on screen),
so an unknown value from a future authoring revision must not be able
to make that claim.

## Test: `find_feature_first_wins_on_audio_count_tie`

On a tie in audio-slot count, the FIRST playlist encountered wins
(consistent with `select_result`'s first-wins tiebreak elsewhere in the
registry) — later playlists only displace the current best on a
STRICTLY greater count.
