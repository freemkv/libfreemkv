# fMP4 muxer (`src/mux/fmp4/mod.rs`) — stub status and design notes

## Status

**STUB**. The muxer can emit the init segment (`ftyp` + a minimal HEVC
`moov` skeleton with one video track) via
[`Fmp4Mux::write_init_segment`], so the shape and call site are
validated, but media fragments (`moof`/`mdat`) are NOT emitted.
[`Fmp4Mux::write_video`] therefore returns
[`Error::Fmp4Unimplemented`](crate::error::Error::Fmp4Unimplemented)
rather than silently accepting and discarding frames. It buffers
nothing, so it cannot accumulate memory.

## Not yet implemented

- `moof` box: `mfhd` (sequence_number) + `traf` (`tfhd` + `tfdt`
  + `trun` with sample sizes, durations, flags, composition offsets).
- `mdat` box: concatenated sample data.
- Fragment cadence: one fragment per GOP or every N seconds,
  whichever comes first.
- HEVC `hvcC` box inside `moov.trak.mdia.minf.stbl.stsd` so the
  init segment is self-describing (`stsd` currently has zero entries).
- Sample-flags computation (sync vs. delta, depends_on, etc.).
- Edit lists / fragment_duration for accurate seeking.

Reference: ISO/IEC 14496-12 §8 (Movie Fragments).

## `wrap_box` size-cast rationale

All callers build tiny init-segment boxes (kilobytes at most), so the
`u32` size never overflows; the saturating cast plus the debug assert
documents and guards that invariant rather than silently emitting a
truncated, structurally corrupt size field. `body` is always
internally constructed here, never untrusted input — a future caller
feeding a multi-gigabyte body trips the debug assert instead of
writing a malformed box.
