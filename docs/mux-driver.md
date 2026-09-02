# `src/mux/driver.rs` — design notes

## Overview

High-level mux driver — the one place that runs the
`construct → headers gate → open sink → pump → finish` pipeline the
consumers (CLI `pipe`/`pipe_disc`, autorip `run_mux`) each hand-rolled.

`mux_stream` DRIVES the existing highway; it does not replace it. For a
file/ISO source it constructs the SAME
[`build_iso_pipeline`](../src/mux/resolve.rs) 3-stage
prefetch → demux → parse chain the consumers build today (the 660 MB/s
highway), reads frames off it exactly where the consumers call
`stream.read()`, and writes them through a
`WRITE_PIPELINE_DEPTH`-deep write `Pipeline` so the latency-bound sink
write overlaps the next read. No wrapper is inserted around the reader or
the frames — the only threads are the three inside `build_iso_pipeline` plus
the single write consumer, exactly as in the consumers today.

## Gate ordering (bug fix)

The `chapters://` / `json://` metadata sinks write their whole file from the
scanned title at `output()` time and consume no PES frames, so they need no
codec headers. The CLI put that short-circuit AFTER the `headers_resolved`
gate (`pipe.rs`), so a metadata export on a title whose video headers never
resolved failed with `MkvInvalid`. Here the short-circuit runs BEFORE the
header pump/gate — by construction a metadata sink can never trip the header
gate.

## `NO_SEND_DEADLINE`

A slow-but-alive downstream — a paused pager, a backpressured pipe, a slow
network sink — must NOT be reported as an interrupted mux, which is exactly
what a finite per-frame deadline would do after that much backpressure.
`None` therefore means "block on backpressure, don't time out", while still
honouring the halt token for Ctrl-C / `/api/stop`.

This is safe because `Pipeline::send_with_halt` slices its wait at
`crate::halt::POLL_INTERVAL` (250 ms) internally and re-checks the halt
token on EVERY slice — halt responsiveness is independent of the deadline
value. So a huge (but finite, `Instant`-overflow-safe) duration blocks
indefinitely on a live-but-slow sink yet still unwinds within ~250 ms of a
halt. ~10 years is far past any real mux; a literal `Duration::MAX` risks
panicking the `Instant::now() + deadline` addition inside `send_with_halt`.

## `MuxOptions::send_deadline`

`None` means no backpressure timeout: the send blocks as long as the
downstream is alive (only a `halt` interrupts it). The CLI's interactive
stdout / network sinks pass `None` so a slow-but-alive consumer (paused
pager, backpressured pipe, slow peer) is never spuriously reported as an
interrupted/incomplete mux — matching the pre-refactor inline
`output.write()` which had no deadline.

## `MuxEvents` event-firing threads

The driver fires `Self::on_output_opened` and the write-side
`Self::on_write_progress` from the driving thread; the reader-side events
(`Self::on_sector_skipped` / `Self::on_batch_size_changed` /
`Self::on_read_error`, plus `Self::on_read_progress`) are fired by the
cloned `EventFn` from the highway's producer thread / the live
`DiscStream`'s read loop.

Progress is split into two callbacks because consumers drive their progress
UI from different sides of the pipeline: the CLI renders from the WRITE side
(bytes finalised to the sink), autorip from the READ side (bytes pulled off
the disc). Keeping them distinct avoids the old ambiguous single
`on_progress(bytes, total)` where the caller couldn't tell which number it
was handed.

## `MuxOutcome::undelivered_streams`

Non-empty means the file does NOT match the pre-mux plan
(`mp4_fit_report`), even with `completed = true`: those streams are
missing. Always empty when the run stopped before the sink was finalised
(nothing was finished, so nothing is known).

## `mux_run_completed`

A clean operator stop (`interrupted`), a wedged/halted finalize
(`finalize_failed` — the write `Pipeline` returned
`Halted`/`PipelineJoinTimeout` from `finish`), or a halt cancellation each
force `completed = false`, so the consumer runs its stop-preserves-staging
path instead of reporting a truncated file as done.

Extracted as a pure fn because the `finalize_failed` branch is otherwise
reachable only through real write-thread wedge timing (the
internally-built `WriteSink` offers no seam to force a `finish` timeout
deterministically), so the mapping is unit-tested here directly.

## `session_mux_keys`

CSS title keys are per-VTS, and `Disc::decrypt_keys` returns the LARGEST
title's VTS key — muxing a bonus title in a DIFFERENT VTS with it
descrambles to corrupt MPEG-PS. `resolve_dvd_title_key`'s crack-guard only
fires when `keys` is `None`, so passing the whole-disc key would skip the
per-title crack entirely. This mirrors the sibling `resolve.rs::input()`
DVD special-case (and the pre-refactor CLI `pipe.rs`). AACS / genuinely-clear
discs resolve from `decrypt_keys()` with no read.

## `resolve_inline_base_map`

Resolves the base AACS key map for an INLINE live-drive mux (the `Session`/
`Live` arms) BEFORE the reader is moved into `DiscStream::new` — the
counterpart to the map resolution `build_iso_pipeline` performs internally
for the file highway.

Under the map-only decrypt model an AACS `DecryptingSectorSource` decrypts
NOTHING until a key map is installed: with no map the AACS arm of the
decrypt path fails loud with `Error::DecryptFailed` on the first content
unit (the deliberate "a reader built without its map is a bug" guard). Both
inline arms used to skip this — `Session` installed no map at all, and
`Live` installed only a caller-supplied forensic FMTS map (`None` for a
plain AACS disc) — so EVERY plain AACS Blu-ray/UHD muxed via the live
single-pass path failed `DecryptFailed` on the first content read. This
resolves + returns the map so the caller can install it via
`DiscStream::with_key_map`.

- AACS keys → resolve (`resolve_mux_key_map`: single-CPS content map,
  multi-CPS per-extent key selection, or FMTS per-segment map) and return
  `Some(map)`. Resolution failure propagates (fail loud), matching the ISO
  path's decrypt gate.
- CSS / clear / `None` → `Ok(None)`: CSS self-cracks per title inside
  `DiscStream::new`, and a genuinely-clear disc needs no map.
- `raw` → `Ok(None)`: ciphertext passthrough, no decrypt step to key.

The `reader` is borrowed only to SAMPLE ciphertext here (the UDF/FMTS probe
and any multi-CPS unit samples); a single-CPS disc — the overwhelming
majority, including every single-key UHD — resolves its map with NO content
read beyond the one-time UDF filesystem probe. The caller then moves the
same reader into `DiscStream::new`; reads are by absolute LBA, so the
sampling leaves no read-position state behind.

## `reader_event_fn`

Cloning the `Arc` into the returned closure is precisely what lets a
borrowed-lifetime consumer's events reach the highway's producer thread — a
`&dyn MuxEvents` borrow cannot satisfy the `'static` bound.

Mapping (real `EventKind` variants):
- `BytesRead { bytes, total }` → `MuxEvents::on_read_progress` (read-side;
  the file highway's only reader event — `total` is the extents' byte total)
- `SectorSkipped { sector }` → `MuxEvents::on_sector_skipped` (live only)
- `BatchSizeChanged { new_size, reason }` → `MuxEvents::on_batch_size_changed`
  (live only)
- `ReadError { sector, .. }` → `MuxEvents::on_read_error`

Sector numbers are the library's `u64`; the `MuxEvents` LBA hooks take `u32`
(the disc's LBA space), so they are narrowed with `as u32`.

## `mux_input_live_uses_inline_discstream_and_applies_key_map` test

`MuxInput::Live` builds the INLINE `DiscStream` (not the highway) AND
applies the forensic `key_map` before reading. Proof is the recorded LBA
set: `with_key_map` rewrites the extent walk via `AacsKeyMap::read_plan` so
the alternate-phase (odd) forensic units are NEVER fetched off the source.
The zeroed reader can't resolve codec headers, so the mux drains to a
`NoStreams`/`MkvInvalid` refusal — irrelevant here; the test asserts only
the read plan the reader actually executed.

Mutation: deleting the `stream = stream.with_key_map(map)` line in the
`Live` arm leaves the extents un-rewritten, so the reader DOES fetch the
dropped odd-phase LBAs and the "never read" assertion below fails. (A
second mutation — routing `Live` through `build_iso_pipeline` instead of
`DiscStream::new` — would not compile against a `Box<dyn SectorSource>` and
drop the inline `fill_extents` retry entirely.)

## `mux_input_live_aacs_without_caller_map_resolves_and_decrypts` test

END-TO-END decrypt on the live single-pass `MuxInput::Live` path with a
plain (non-FMTS) AACS disc and NO caller-supplied key map — the exact shape
of `freemkv rip disc://…` and autorip's non-FMTS single-pass. The driver's
`Live` arm must RESOLVE + INSTALL the base AACS key map itself; the unit
then decrypts to a valid audio PES and the mux drains and finalises.

This is the regression guard for the confirmed bug: without the map the
AACS `DecryptingSectorSource` fails `DecryptFailed` on the first content
unit and no AACS disc could ever be live-muxed.

Mutation: deleting the `resolve_inline_base_map` call (or the
`stream = stream.with_key_map(map)` install) in the `Live` arm leaves the
reader mapless → the first content batch cannot decrypt (root cause
`DecryptFailed`, surfaced through `fill_extents`' non-skip read-error path
as a `DiscRead`) → `mux_stream` returns `Err` and `out.completed` is never
reached (verified: the mux aborts instead of finalising).

## `mux_input_session_aacs_without_caller_map_resolves_and_decrypts` test

END-TO-END decrypt on the live single-pass `MuxInput::Session` path — the
exact shape of `freemkv rip disc://…mkv`. The `Session` arm runs the SAME
sequence as `Live` (take_reader → resolve_inline_base_map → DiscStream →
with_key_map), but until now had NO end-to-end coverage because a real
`DiscSession` needs a live `Drive`. Using the `#[cfg(test)]`
`from_parts_for_test` constructor, a genuinely-AACS-encrypted unit muxed
through the `Session` arm must resolve+install the base key map itself and
DECRYPT to a valid audio PES.

Mutation: dropping `stream = stream.with_key_map(map)` (or the
resolve_inline_base_map call) in the `Session` arm leaves the reader
mapless → the content batch cannot decrypt → the mux aborts (`Err`) and
`out.completed` is never reached.

## `finalize_failed_forces_incomplete_outcome` test (FIX 3)

A mux whose read side drained cleanly (`interrupted = false`, halt not
cancelled) but whose write pipeline WEDGED on finish (`finish_with_halt` →
`Err(Halted | PipelineJoinTimeout)` → `finalize_failed = true`) must fall
through to `completed = false` — never surface a truncated file as a
finished rip.

Mutation: dropping `finalize_failed` from `mux_run_completed`'s condition
makes `mux_run_completed(false, true, false)` return `true` → this fails.

## `mux_input_session_out_of_range_title_is_clean_error_not_panic` test (FIX 4)

`MuxInput::Session` with a `title_index` past the disc's title count must
surface a clean `Error::MuxTrackRange` (code E9011), NOT panic on the
out-of-range `titles.get(idx)`. Everything else is valid (disc scanned,
reader staged) so the range guard is the sole failure.

Mutation: replacing the `.ok_or(MuxTrackRange…)?` guard with `.unwrap()`
panics on the out-of-range index → this test fails.

## `HEADER_BUFFER_CAP_BYTES`

Normally `headers_ready()` resolves after a handful of frames; a
damaged/undecryptable title whose video `codec_private` never resolves
would otherwise grow the pre-headers buffer without bound (the whole
30-90 GB title, one PES frame at a time) until the process is OOM-killed.
512 MiB is far more than any real codec-private resolution needs but small
enough to fail fast rather than swap the box to death. Once exceeded the
mux is refused with `Error::MuxHeaderBufferExceeded` — its OWN code, not
the headers-never-resolved gate's `Error::MkvInvalid`, which
`error::is_skippable_title_stub` reports as a skippable stub. Mirrors
autorip's pre-refactor `HEADER_BUFFER_CAP_BYTES`.
