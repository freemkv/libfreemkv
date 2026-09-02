# src/mux/select.rs — internal notes

Overflow rationale relocated here by the comment-guard so the in-file
comments can stay short. Each section is pointed to by a one-line
`// See docs/mux-select.md — <topic>` comment at the corresponding site.

## Module rationale

The demux pipeline is declaration-driven: every demux table
(`build_demux_state` in `mux/resolve.rs`, `DiscStream::new` in `mux/disc.rs`)
is built from the input `DiscTitle`'s `streams` list, and the MKV writer
builds its track headers + `codec_privates` from that same list. A PID not
declared there is never tracked, extracted, or written. So "which streams to
keep" is already a capability of the pipeline — it just has no public knob.

`StreamSelection::apply` is that knob: prune the `DiscTitle.streams` list
(video always kept) BEFORE the mux path finalizes the title, and everything
downstream — track headers, `codec_privates`, PID routing, frame emission —
follows from the pruned list by construction, with zero scattered PID
checks. This is language-agnostic: PIDs, not languages (the language→PID
mapping is the caller's/engine's policy).

## `StreamSelection::apply` rationale

The parallel `codec_privates` vec is pruned in lockstep when it is
populated (it is empty on a freshly-scanned title, non-empty only if a
caller pre-filled it) — by index, whatever its length, since it is consumed
positionally and a partial prune would attach the wrong codec-private to a
retained track.

`Error::SelectionPidUnknown` is returned rather than silently emitting an
MKV missing a requested track — fail loud on a caller bug (e.g. a stale
scan) instead.

## codec_privates length-mismatch regression (test)

A `codec_privates` vec that is NOT exactly stream-length must still be
pruned in lockstep. `m2ts.rs::create` documents a longer-than-streams vec
as a benign shape ("ignore any trailing entries that exceed the track
count"), and the vec is consumed positionally, so skipping the prune left
`codec_privates[i]` describing a stream that is no longer at index `i`.

Regression: with one trailing extra entry the prune was skipped entirely
and index 1 — the retained `fra` audio — resolved to `eng`'s record, so
the muxer attached the wrong codec-private to the track. No error, no log.

## Wrong-class filter PID regression (test)

A PID listed in the wrong class's filter must fail loud, not validate and
then quietly vanish. Validation used to scan both audio and subtitle
streams, so an audio filter naming a subtitle PID passed — and `keeps`
then matched it against audio streams only, dropping the requested track
from the output with no error. That defeats the documented "fail loud
rather than silently emit an MKV missing a requested track" contract.
