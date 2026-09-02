# `dropgate` — audio drop bookkeeping rationale

## Keep/drop rule

A clean mux keeps every frame it can and drops the ones it can't — video
always survives (it's inter-frame predicted; a per-frame drop would cascade,
so video resyncs/conceals instead), audio keeps every decodable access unit,
and a damaged audio AU is dropped rather than shipped as a decoder-choking
glitch.

## Detection is per-codec

The DETECTION is inherently per-codec — each format carries its own
authoritative corruption check (DTS: the core sync/header parse per ETSI TS
102 114; AC-3: the header CRC per ETSI TS 102 366; FLAC: the frame CRC-16;
…). `DropTally` only carries the UNIFORM response so every audio parser
behaves identically:

1. **Count** kept vs dropped AUs and the dropped duration.
2. **Log** every drop (fail-loud, never silent) — a per-drop trace plus a
   once-per-track aggregate at `warn` so it surfaces without debug logging.
3. **Whole-track fallback**: once a track is judged mostly undecodable, latch
   a poison flag so the remainder is dropped too (a track that damaged isn't
   worth muxing).

## Sync preservation

**Sync preservation is the caller's responsibility**, not this type's: the
parser must advance its PTS clock across a dropped AU exactly as it would for
an emitted one, so a drop becomes a silence gap and never a shift of the
following audio. See `DtsParser`'s `stamp_pts` call ordering for the pattern.

## `verified_dropped` vs `dropped`

`verified_dropped` counts AUs dropped because they were INDIVIDUALLY verified
undecodable (a failed CRC/header/parity check); only these feed the
whole-track poison verdict. `dropped` also counts *collateral* drops — AUs
discarded as a consequence of one corruption (TrueHD's resync-forward run, or
a poisoned track) — which must NOT amplify a few real errors into a false
whole-track loss.

## `record_collateral_drop`

A collateral drop is caused by another AU's corruption (TrueHD's
resync-forward run to the next major sync, or an already-poisoned track), not
by being individually verified undecodable. It is counted and logged for the
drop report, but deliberately does NOT feed the poison verdict, so one
corruption event can't amplify into a false whole-track loss.

## `maybe_poison`

Whole-track fallback: after enough AUs to judge, if more than half were
dropped the track is too damaged to be worth muxing — latch `poisoned` and
log it loudly once. The minimum-sample gate keeps a short damaged burst from
poisoning an otherwise-good track. Judge on VERIFIED drops vs all AUs seen: a
track is only poisoned when a majority of its access units are individually
undecodable — not when a couple of corruption events forced long collateral
resync runs.

## Test: `interleaved_keeps_are_in_the_poison_denominator`

The poison verdict is a RATIO — verified drops against every AU seen — so
the kept count is half of it. `does_not_poison_a_mostly_good_track` records
its keeps AFTER the single drop, and `maybe_poison` only runs inside
`record_drop`, so the keeps are never in the denominator when the verdict is
actually computed: that test passes even with the kept count never
incremented. Interleaving them puts the kept count on the critical path,
where losing it turns the ratio into "verified drops vs verified drops" —
always >50% — and silently discards a healthy track.
