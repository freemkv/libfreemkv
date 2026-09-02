# diag.rs — scan diagnostics rationale

## Why this module exists

A bug report log must be self-diagnosing: everything needed to explain *why*
freemkv made the choices it did at scan must be in the log, in a compact,
machine-parseable form. `diag.rs` emits one terse line per row (title, cell,
stream, decision) under the `tracing` target `freemkv::diag`, which the CLI
routes to `log.txt` when `--log-level 3` (debug) is set.

## Format conventions (stable, greppable)

- Every line is prefixed by a `tag=` so a log scraper can filter (`disc`,
  `title`, `dvd.cell`, `dvd.vattr`, `dvd.aattr`, `bd.clip`, `bd.mark`, `aacs`,
  `stream`, `decision`).
- Raw bytes are shown as `0xNN` next to their decode so a wrong decode is
  obvious against the raw value.
- This module only READS already-parsed scan state — it never re-reads the
  disc and never mutates anything.

## DVD per-cell table vs. Disc-level dump

The DVD per-cell table (with the raw cell-category byte) is emitted from the
IFO scan itself (`dump_dvd_cells`), because the per-cell `ifo::DvdCell` detail
is lowered away before the `Disc` is built. The `Disc`-level dump
(`dump_disc`) covers everything that survives lowering: titles, streams, the
picked main feature, and AACS state.

## `dump_dvd_substream_probe`

Emits the ACTUAL per-physical-sub-stream AC-3 channel counts read off the VOB
during the mux-time sub-stream probe (the Silence-of-the-Lambs wrong-stream
fix). This is the ground truth the IFO nibble is compared against: each row is
`sub_id=0x8x channels=N` for a physical `private_stream_1` AC-3 sub-stream
whose first frame was decoded. An empty probe (scrambled / unreadable / short
VOB) logs a single `probed=0` line so the absence is explicit in a bug log.

Self-sufficiency: with `tag=dvd.aattr` (the IFO's declared sub_id + claimed
channels) and these `tag=dvd.substream` rows (the physical reality), a bug log
alone shows whether the ordinal `0x80` actually carries the declared channel
layout — no disc needed to diagnose a wrong-substream rip.

## `dump_mkv_track`

Emits the MKV `TrackEntry` elements the muxer is about to WRITE for one track
— the Windows-fps-class metadata (FlagInterlaced, FieldOrder,
DefaultDuration, DefaultDecodedFieldDuration, Display dims) plus the
codecPrivate as hex. With this row a bug log alone is enough to verify why
Windows Explorer reports a given frame rate for an interlaced SD track: the
container values that drive its fps derivation are all present, no disc and
no MediaInfo needed.

`track_number` is the 1-based MKV track number; `track` is the built
`crate::mux::mkv::MkvTrack` whose fields map one-to-one onto the emitted
elements (see `MkvMuxer::new`).

## `main_feature_reason`

The `reason=` token on the main-feature decision row is DERIVED from
`Disc::CANONICAL_TITLE_ORDER_KEYS`, which lives beside the comparator that
actually implements them — never restated here. The previous hand-written
copy drifted (it advertised a `fewest-clips` key the comparator had replaced
with largest-physical-size), which made the self-diagnosing log explain the
pick with a rule the code does not apply. A diagnostic that disagrees with the
decision it documents is worse than no diagnostic.
