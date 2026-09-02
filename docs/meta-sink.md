# `meta_sink` — `json://` document construction

## `title_json` — single serialization source of truth

The per-title / per-stream schema comes from the normalized [`TitleProfile`]
(the same typed view `Disc::profile` exposes), serialized directly —
`json://` and any downstream consumer share one schema rather than a
hand-built one that can drift.

**Schema note.** This replaces the previous hand-built `streams` array. The
stable editorial meaning is preserved, but the shape is normalized: the one
`streams` array (tagged with `kind`) becomes three typed arrays (`video` /
`audio` / `subtitles`); subtitle `qualifier` becomes the booleans `forced` +
`sdh`; audio `purpose` becomes `commentary` + `descriptive`; and per-track
`default` is now hoisted (first non-secondary video/audio). Per-stream fields
the normalized profile intentionally omits (`pid`, `width`/`height`,
`color_space`, `measured_cicp`, `sample_rate`, `channel_count`,
`mvc_dependent`) are no longer emitted; they live on the richer scan model,
not on this format-agnostic view.

`json://`-only extras that are NOT part of the normalized profile — the
title's `clips` and the full `chapter_marks` list — are spliced back on so
those consumers do not regress. (`chapters` from the profile is the marker
COUNT; the full list is under `chapter_marks`.) `json://` is a per-title sink
with no disc context, so `index` is `0` and `is_main` is `false`; the
disc-level [`crate::disc::DiscProfile`] populates those correctly.

## Write-only sinks: `read()` semantics

`chapters://` and `json://` are write-only sinks: the whole file is emitted
at `create()` and there is nothing to demux back. `read()` returning
`Ok(None)` instead of the write-only error would make a caller that pointed
a mux INPUT at one of these URLs see a clean empty stream — the exact shape
of the shipped "empty title, exit code 0" defect. It must refuse with the
numeric code `E_STREAM_WRITE_ONLY`.
