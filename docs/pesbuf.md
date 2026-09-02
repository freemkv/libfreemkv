# pesbuf: one accumulation buffer for parsers that assemble access units across PES packets

A buffering parser has to answer the same question for every unit it emits:
*which PES contributed this unit's FIRST byte?* Its timestamp comes from
that PES, and so does the source byte offset that identifies which clip of a
multi-clip title the unit belongs to. The trailing PES packets that complete
the unit carry their own, later, values which must not override it.

That question was answered three different ways. DTS kept a deque of
`(offset, pts)` markers and took the one covering offset 0 — correct. AC-3
kept a single carry-over timestamp. TrueHD kept its own. None of them
carried the source offset at all, so provenance existed only for video, and
a title whose clip marks could not be read from timestamps alone had nine
audio and subtitle tracks with nothing to place them by.

Three spellings of one rule is how they drifted, so this is the one place it
lives. The buffer owns the bytes AND the marks, and returns a PES's facts
together — a parser cannot take the timestamp from one PES and the source
from another, because it does not assemble them itself.

## `PesFacts`

Returned as a unit so a caller cannot mix fields from different packets.
The timestamps are carried RAW, as the packet had them. This type answers
*which packet* a unit's facts come from — the question that was being
answered three different ways. How a given codec derives a timestamp from
that packet stays the codec's business: DVD subtitles read `pts` only and
fall back to 0, most audio takes `pts.or(dts)`. Deriving it here would have
changed those semantics silently while fixing provenance.

## `PesFacts::of`

Every parser reads its frame's timestamp, source and discontinuity from
a `PesFacts` — never off a `PesPacket` field directly — so the three can
never be taken from different packets. Parsers that assemble units
across packets get theirs from `PesBuf::front`; this is the same value
for a parser whose unit begins in the packet it is handed.

## `PesFacts::presentation_ns`

PTS and DTS are not two spellings of one value: PTS is when to display,
DTS is when to decode, and for a stream that REORDERS (video carrying
B-frames) they differ, so reading DTS as a presentation time would be
wrong. Reordering is handled by the video path, which reconstructs
display order rather than calling this.

For everything that reaches here — audio and subtitles — there is no
reordering, so DTS *is* the presentation time, and falling back to it is
reading the same value from whichever field the packet used. A fallback
for a missing field, not a second rule: dvdsub read `pts` alone and
returned 0 for a packet that carried only DTS.

## `PesBuf::drain`

The mark covering the new front byte is RETAINED at offset 0 even though
its packet started earlier: those bytes are still that packet's. Dropping
it would attribute the remainder of a straddling unit to whichever packet
happened to start next — precisely the misattribution this type exists to
prevent, and it would land at a clip boundary, the one place it matters.
