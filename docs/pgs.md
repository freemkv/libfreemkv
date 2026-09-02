# HDMV PGS subtitle parser (`src/mux/codec/pgs.rs`)

## Module overview: why display sets need a synthesized duration

For Matroska output the parser collapses a display PCS / clear PCS pair into
one block with `BlockDuration` set to `clear_pts - display_pts`. Without a
duration, hardware players linger on the last bitmap until the next subtitle
replaces it — which can be many seconds, and on a disc where the final
subtitle has no follower, until end of file.

## `display_set_is_forced`

The mux uses this to detect a *forced-narrative track* (every displayed
subtitle forced) without relying on the disc's vendor label metadata, so
forced subs are flagged `FlagForced` even on discs that carry no such blob.

## `demotable`

Full rationale and measured track shapes:

* Promotion (0 → 1) needs no gate: it rests on positive evidence (every
  display set carried `forced_on_flag`). Demotion rests on an ABSENCE, and an
  absence is only meaningful if the flag is in use at all. Measured: discs
  exist on which NO track carries `forced_on_flag`; there, "this track has no
  forced display sets" is a fact about the authoring house, not about the
  track, and demoting on it would strip a correct forced label from every
  track on the disc.
* `DEMOTE_MIN_DISPLAY_SETS`: a track must have shown at least this many
  display sets before "none of them was forced" is allowed to contradict a
  vendor forced label. Absence is weak evidence on a handful of sets: a
  genuine forced-narrative track is SMALL (measured shape: tens of display
  sets for a whole feature), so a couple of unflagged sets is exactly what
  one looks like on a disc whose authoring never sets the flag.
* `DEMOTE_MIN_DISPLAY_SHARE_DIVISOR`: the track must also carry at least this
  fraction (1/N) of the display sets of the busiest subtitle track on the
  disc. Measured: a dedicated forced track carries a low-tens count of
  display sets for a whole feature, a full dialogue track carries one to two
  thousand — two orders of magnitude apart. A track sitting within a quarter
  of the busiest track's count is a full track, whatever its label says; a
  track at one percent of it is the forced-narrative track its label claims
  and must keep that label.
* A track that itself mixes forced and non-forced display sets is the
  stronger form of "flag in use": it shows the authoring house making that
  distinction deliberately. The shape test still applies to it — a SMALL
  track with a couple of flagged sets is a forced track whose authoring
  flagged some of its signs, and demoting it would be exactly the mistake the
  shape test exists to prevent.

See also `docs/pgs-forced-probe.md` for the `info`-time probe that reuses
this same classifier.

## Test notes

`observed_stays_false_until_a_real_display_set_is_seen`: `observed()` is the
probe's "did I actually see any PGS content?" signal. When false the track's
forced state is UNKNOWN and the probe leaves whatever flag the disc's own
metadata supplied alone; when true, the probe overwrites that flag with its
own verdict. A tracker that always claims to have observed something would
let an unread or undecrypted subtitle track — where `is_forced()` is
vacuously false — overwrite a correct vendor "forced" flag with "not forced".

`a_mixed_track_corroborates_the_flag_itself`: a track that itself mixes
forced and non-forced display sets needs no corroboration from a sibling —
the flag is demonstrably in use ON THIS TRACK. Models a busy track labelled
forced that flags one or two of its hundred-odd display sets.

`a_lone_segment_emitted_directly_still_carries_provenance`: a lone non-PCS
segment with a PTS is emitted straight through rather than accumulated, and
it must still carry provenance. This path was missed on the first pass and
showed up on a real disc as a subtitle track with no source offset — the one
track out of forty that could not be placed by byte.
