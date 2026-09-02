# `src/mux/videomap.rs` — design notes

Each per-picture record's coding truth ([`PictureInfo`], off `frame.coding`)
and source provenance ([`SourcePos`], off `frame.source`) come from what the
highway already stamps on the frame — this module never re-parses the
elementary stream itself.

`VideoMap` is a STANDALONE PRIMITIVE, deliberately decoupled from any one
sink:

- The `fvi://` sink (`crate::mux::fvi_sink`) owns a `VideoMap`, appends each
  video `PesFrame`, and serializes it.
- The same `VideoMap` can later be populated as a side-channel during ANY
  mux (e.g. `iso → mkv` while ALSO emitting a `.fvi` sidecar), and reused for
  seek-indexing, recovery loss-mapping, and diagnostics.

A different output format would be a DIFFERENT sink reusing this same model,
not a pluggable encoder inside `videomap.rs`.

## Regression: `fvi_colour_follows_hdr_and_measured_cicp`

An HDR10 BT.2020 title's real transfer is PQ (16); a measured CICP triplet
is authoritative and copied through verbatim. Before the fix, the FVI
`Colour` reported transfer=14 (the SDR default for BT.2020) while the MKV
container reported 16 — two sinks of one title disagreeing on the colour
code points.

