# src/mux/disc.rs — internal notes

Overflow rationale for internal (non-`pub`) items in `src/mux/disc.rs`,
relocated here by the comment-guard so the in-file comment can stay short.
Each section is pointed to by a one-line `// See docs/mux-disc.md — <topic>`
comment at the corresponding call site.

## commit_read

Commits a SUCCESSFUL `read_sectors` into the read buffer and advances the
extent cursor. `got` is the byte count the source reported (already clamped
to `bytes`, the full span requested for `sectors`).

The full case (`got == bytes`) is the only one every in-tree `SectorSource`
produces — they are all full-or-error — and it behaves exactly as before.

A SHORT read (`got < bytes`) used to be handled inconsistently: the returned
count was trusted for `buf_valid`, but `current_offset` still advanced by the
full requested `sectors`. The undelivered tail therefore vanished from the
muxed title with no error, no `SectorSkipped` event and no `lost_bytes` — a
silent hole, indistinguishable from clean output, in a single-pass path that
has no later pass to recover it. It is handled here rather than trusted away
because invisible data loss is the one outcome this stream must never
produce.

The tail is NOT re-read from a smaller offset: `current_offset` must stay on
an AACS unit boundary (`unit_align`), and resuming mid-unit desyncs the
decrypt of everything after it — the same reason the failed-unit skip branch
below advances by the whole unit. So the gap is either

- a hard [`crate::error::Error::DiscRead`] (E6000) when the caller has NOT
  opted into holes, or
- zero-filled and ACCOUNTED under `skip_errors`, identically to a failed
  unit: the stale buffer tail is cleared, `errors` / `lost_bytes` are
  charged, and a `SectorSkipped` event is emitted.

## a_long_run_of_empty_extents_does_not_recurse_per_extent

Nothing filters `sector_count == 0` out of a UDF/MPLS extent list, so a
malformed disc can declare a long run of empty extents. `fill_extents` used
to skip each one with a self-recursive call, costing a stack frame apiece;
Rust does not guarantee tail-call elimination, so a few thousand of them
overflowed the stack. A stack overflow aborts the process — it is not
catchable and takes the whole `autorip` service down — where the iterative
form just walks off the end and reports EOF.

Run on a deliberately small stack so the frame cost is unmissable: the
recursive version dies here, the loop finishes in microseconds.

## bad_sector_keeps_its_identity_across_the_prefetch_channel

REGRESSION (round-4 audit): an ordinary MEDIUM ERROR bad sector must keep its
identity when it crosses the prefetch producer channel — the same `DiscRead`
with its SCSI status, NOT a transport failure.

`PrefetchedSectorSource::read_sectors` re-wrapped every error that crossed
the channel as `Error::IoError`, and `is_scsi_transport_failure` matches
`IoError` (the wedged-USB-bridge arm). So a bad sector reached `fill_extents`
looking like a dead bus and aborted the pass with a fabricated status 0xFF —
the exact inverse of what that short-circuit exists for, and it told the
user to power-cycle a healthy drive.

Asserted on the source, not on a `fill_extents` skip: the producer thread
exits for good after sending an error, so nothing downstream of it can
genuinely recover the rest of the title (see
`dead_prefetch_producer_does_not_silently_zero_fill_the_title`). An assertion
that the pass continues could only ever have been satisfied by fabricated
zeros.

## dead_prefetch_producer_does_not_silently_zero_fill_the_title

The prefetch producer thread terminates PERMANENTLY on its first read error,
so once one bad sector has crossed the channel the source can never deliver
another byte. Driving `fill_extents` to exhaustion after that must NOT look
like a completed pass: every remaining sector would be fabricated zeros, and
DATA LOSS MUST NEVER LOOK LIKE SUCCESS.

The expectation is the product rule, not the code: a source that is
permanently out of data must report that, not answer `Ok(0)` forever — which
`commit_read` legitimately reads as an ordinary short read and zero-fills.

## errors_reports_frames_the_resync_gate_dropped_after_the_gap_resolves

Frames the B1 resync gate discards must reach `errors()`, including after
the gap RESOLVES.

`ResyncGate::dropped` is zeroed the moment a keyframe disarms the gate, and
the only EOF warning fires for gates STILL armed — so a mid-title gap that
resolves left no trace anywhere. That is the common case: most gaps do
resolve. A rip with several concealed gaps reported 0 errors and 0 lost
bytes while whole GOPs were discarded.

This asserts the accessor, not the gate's own counter, because `errors()` is
the only channel through which a caller learns anything went wrong.
