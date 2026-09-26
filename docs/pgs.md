# HDMV PGS subtitle parser (`src/mux/codec/pgs.rs`)

## Display and clear are separate decoder events

A visible composition (PCS with objects) starts a subtitle. An empty
composition (PCS with zero objects, followed by its remaining segments and
END) clears it. Both sets must survive ripping. The parser also computes
`BlockDuration = clear_pts - display_pts` for the visible set, retaining the
existing fallback duration when the actual end is unavailable.

Previously, the parser discarded the empty PCS and relied solely on
`BlockDuration`. FFmpeg's PGS decoder sets `end_display_time` to `UINT32_MAX`
and expects another composition to replace or clear it. The generic decoder
only uses packet duration when `end_display_time` is zero. A player using
that decoded lifetime can therefore leave a subtitle visible until the next
one, however large the gap. See the [PGS decoder][pgsdec] and
[generic subtitle decoder][decode].

Empty compositions are now accumulated with their WDS/END continuation
packets, retaining the opening PCS's timestamp and source position. A
complete clear is emitted at END without waiting for the next subtitle.
Its duration is zero (the MKV writer rounds this to one timestamp tick);
it changes decoder state immediately and has no visible bitmap to expire.
Clear sets do not count toward forced-track classification.

### Relationship to freemkv/freemkv#52

[#52] reports missing timestamps during FFmpeg transcoding with subtitle
stream copy. Adding a duration to every block does not, by itself, guarantee
valid packet timestamps after bitstream filtering. FFmpeg's Matroska muxer
automatically applies [`pgs_frame_merge`][merge]. When it merges separate
WDS and END packets, it copies packet properties from the PCS-containing
packet. Dropping that PCS leaves an orphaned set whose merged packet can
have no timestamp even though each input MKV block had one.

Running the new FFmpeg tests against the old parser reproduces both reported
warnings (unset timestamps and fabricated PTS), and decodes the supposed
clear events as visible compositions. Both tests pass with the fixed parser.
Preserving the complete clear set addresses that mechanism as well as the
lingering bitmap. This does not establish that every playback/transcode
problem in #52 has the same cause; verification against the reporter's
source is still needed.

### Regression coverage

`src/mux/codec/pgs_tests.rs` builds decoder-valid synthetic PGS sets with a
2x2 bitmap; no movie/disc assets are needed. Coverage includes an hour-long
gap between forced subtitles, split/timestamp-less continuations, leading
and repeated clears, byte preservation, provenance, incomplete sets,
forced-track classification, and a parser → MKV → reader round trip.
The `.sup` writer tests also cover original clear precedence and fallback
clears for older MKVs carrying only durations.

Two additional Rust tests invoke `ffprobe`/`ffmpeg`: one checks decoded
empty compositions at the actual clear times, the other stream-copies the
MKV and checks warnings, timestamps, corruption flags and decoded clears.
These are explicitly ignored in normal local runs because they require
external binaries. The QA workflow installs FFmpeg on Linux and runs them
against the release build; dev CI runs the pure Rust coverage:

```sh
cargo test --lib pgs
cargo test --release --lib mux::codec::pgs::lifecycle_tests::ffmpeg_ -- --ignored
```

[pgsdec]: https://github.com/FFmpeg/FFmpeg/blob/master/libavcodec/pgssubdec.c
[decode]: https://github.com/FFmpeg/FFmpeg/blob/master/libavcodec/decode.c
[merge]: https://github.com/FFmpeg/FFmpeg/blob/master/libavcodec/bsf/pgs_frame_merge.c
[#52]: https://github.com/freemkv/freemkv/issues/52

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
