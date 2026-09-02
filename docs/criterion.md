# `src/labels/criterion.rs` — internal notes

## `assign_stream_numbers`

Assign a 1-based stream number per `StreamInfo`, parallel to `infos`.

A stream mapped in `playbackconfig.xml` (`stream_map`) keeps its
mapped number. Streams with no mapping (absent or incomplete
`playbackconfig.xml`, or an unmatched `StreamInfo_ID`) are numbered
1-based per type — but the fallback counter SKIPS any number already
claimed via the map, so a synthesized number can never collide with a
map-assigned one. (Both numbering domains are 1-based per type, and
`apply_labels` matches on `(type, stream_number)`, so a collision
would mislabel tracks.)

Returns `None` when the 1-based stream-number space is exhausted —
every number in `1..=u16::MAX` for that type is either already
claimed by the map or already synthesized. That is unreachable on
real media: the BD STN_table carries at most 32 primary audio and 32
PG streams per playlist, so the 65535-wide space leaves >2000x
headroom. It IS reachable from a crafted `streamproperties.xml`
listing >65535 stream entries, and the only correct answers there are
"fail the parse" or "emit colliding numbers"; we fail.

The skip search is bounded by the numbering space itself: a `u16`
`saturating_add` here parked the counter at `u16::MAX` forever
whenever the map also claimed `u16::MAX`, turning an overflow guard
into a hang that `apply()`'s `catch_unwind` cannot interrupt. The
counters are therefore widened to `u32` so the skip loop strictly
increases toward a fixed ceiling (guaranteeing termination) and
exhaustion is reported rather than absorbed.

## Test: immunity pin, section-boundary half

Covers `an_unterminated_stream_element_shortens_the_list_it_cannot_extend_it`.

`parse_stream_infos` emits one `StreamInfo` per `*StreamInfos` element
unconditionally — no filter, no `continue` — so an element whose
fields are missing or unrecognized still occupies its position, and
`assign_stream_numbers` still spends a number on it.

That is the property that keeps this parser out of the failure mode
where a skipped entry pulls every later label one stream forward. It
is load-bearing for the fallback path specifically: with no
`playbackconfig.xml` the numbers come purely from position in this
list, so dropping an element there would shift the rest.

Mutation: skip elements with an empty `ID`/`LangInfoID` → the two
real audio streams renumber to 1 and 2.

Each stream here is one closed XML element, and every field is read
out of `&text[start..end]` — the range `xml::find_element` returned —
so one element can never absorb the next one's fields, however the
document is malformed around it. Contrast the flat-string walk in
pixelogic, where a section whose end marker is missing keeps
consuming entries as STN slots.

The missing-boundary case fails closed. An element with no close tag
of its own ends at the NEXT close tag, so it absorbs the element
behind it — the list comes back SHORTER. It cannot come back longer:
nothing outside a returned range is ever read as a stream, and
`find_element` yields `None` rather than a range running to EOF when
no close tag exists at all. A malformed document can cost this parser
a slot; it can never invent one.

Mutation: read fields from the document rather than the element's
range, or let a close-less element run to EOF → the trailing elements
re-enter the list as extra streams.

## Test: `exhausted_numbering_terminates_instead_of_looping`

A crafted `streamproperties.xml` can drive the fallback counter to
the top of the 1-based u16 stream-number space and then present one
more unmapped stream whose successor number is also claimed by the
map.

This must TERMINATE. The bound is the numbering space itself, so the
assertion is on the spec-derived exhaustion behaviour (`None`), not
on any tunable constant. Run on a worker thread with a deadline so a
non-terminating loop fails the test in 20 s instead of hanging CI.

## Test: `full_u16_numbering_space_is_usable_and_unique`

The whole 1-based u16 space must remain usable: 65535 unmapped audio
streams get 65535 distinct numbers with no panic and no wrap. The
literals here are the JVMS-independent, spec-derived size of a u16
1-based numbering domain, not a tunable cap.
