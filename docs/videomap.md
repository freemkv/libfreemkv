# `mux::videomap`

Reusable, pure-data per-picture video index (the FVI logical model)
consumed by [`fvi_sink`](../src/mux/fvi_sink.rs). Serialization-independent.

`#[allow(dead_code)]`: the `VideoMap` accumulator is a standalone primitive
staged for the side-channel (mux-while-indexing) reuse described in its
module doc; the `fvi://` sink today builds `PictureRecord`s directly, so
the accumulator is covered only by its own unit tests until that wiring
lands.
