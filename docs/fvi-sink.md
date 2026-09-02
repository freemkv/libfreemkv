# `fvi://` sink notes

Rationale and design notes for `src/mux/fvi_sink.rs` that don't belong in the
public rustdoc contract.

## Relationship to `VideoMap`

The sink is a thin consumer of the reusable, pure-data
[`VideoMap`](crate::mux::videomap) model
([`MapHeader`](crate::mux::videomap::MapHeader) /
[`PictureRecord`](crate::mux::videomap::PictureRecord)): it builds the header
from the title, then writes one record per video
[`PesFrame`](crate::pes::PesFrame) straight to disk. Nothing here re-parses
the elementary stream, and the whole index is never buffered.

Serialization is inlined in the sink itself.

## Extensibility

A different output format would be a DIFFERENT sink reusing the same
`VideoMap` model (e.g. a future `fvi2://`), not a pluggable encoder —
extensibility is by adding a sink, like every other sink in this crate.

## `write_fvi_record`: coding-derived members (§7.1)

Coding-derived members (§7.1) are produced via the codec-agnostic
`PictureInfo` accessors, never the raw bitstream: `field_order` is emitted
only when measured (OMITTED on codec-type-only HEVC/H.264/VC-1),
`progressive` only when signalled.

## `write_fvi_record`: `dts` / future `recovered` member

`dts` is MAY per the format spec — the highway carries no DTS on a frame, so
it is always omitted.

TODO(provenance→recovery join): a `recovered` MAY member (sweep/patch mapfile
overlap with this AU's `src`) belongs here; unreachable at `PesFrame`.
