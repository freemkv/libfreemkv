# Content-based forced-subtitle detection for PGS tracks

## Module overview

`freemkv info` and the muxer must agree on which subtitle tracks are forced.
The muxer derives it from the PGS `forced_on_flag` while muxing a rip; this
module gives `info` the SAME verdict up front by reading the title's PGS
streams and feeding them through the one shared classifier
(`crate::mux::codec::pgs::ForcedTracker`) — so the two never diverge.

WHERE it reads is the whole design. A track is forced iff EVERY display set
carries `forced_on_flag`, so one non-forced set disproves forced for good,
while proving forced needs the whole track — and the tracks that are
expensive to prove are the cheap ones to read (a forced-narrative track is
tens of display sets; a full dialogue track is thousands). A bounded budget
spent on the title's HEAD therefore learns nothing at all: a feature's
subtitles begin minutes in, past the end of any affordable prefix. The budget
(`PROBE_BUDGET_SECTORS`) is instead SPREAD over each extent as sample windows
(`plan_windows`), so every window is an independent chance to catch a display
set.

Cost: the budget is a hard ceiling per call. A run stops early once no track
can still change its outcome — disproven, and with no wrong forced label left
to correct.

Contradicting a label: content may CLEAR a vendor forced flag as well as set
one, but only behind `crate::mux::codec::pgs::demotable` — an absence of
`forced_on_flag` proves nothing on a disc whose authoring never sets it.

Encrypted content: the probe reuses whatever `SectorSource` the scan holds.
With a decrypting source it sees real PGS; without keys it reads ciphertext
and observes no display sets, in which case it leaves each track's existing
(vendor-label-derived) forced flag untouched rather than asserting anything.

Truncated reads: the probe is best-effort, but "best-effort" must not mean
"assert a verdict from an arbitrary prefix". `StopReason` records why the
read loop ended and narrows what may be asserted accordingly.

## `CHUNK_SECTORS` (read chunk size)

A whole number of AACS aligned units (3 sectors / 6144 B), because with a
decrypting source — the case this module's doc promises — every read must
begin on a unit boundary measured from the extent base or
`DecryptingSectorSource` rejects it outright with `DecryptFailed`. At 1024
(`1024 % 3 == 1`) every chunk after the first drifted off the boundary, so
content-based forced detection was unreachable past the first chunk of an
AACS disc. 1023 = 341 units.

## `STALL_RETRY_LIMIT`

How many times a read that came back with less than one AACS aligned unit —
so the read position could not advance without leaving the unit grid — is
retried at the same LBA before the run is declared truncated. A couple of
retries covers a source whose batching straddles the request (a short call
followed by a satisfying one); a source that can never yield a whole unit
must not spin, so the count is small and the stop is `ReadFailed`
(inconclusive, not memoised).

## `PROBE_BUDGET_SECTORS`

The probe's natural exit is "every track has shown a non-forced display
set", which a genuinely FORCED track never satisfies — so on the common
authoring (a forced-narrative track for foreign dialogue) the loop would
otherwise read the title's whole extent set, tens of GB, at optical-drive
speed.

UNCHANGED from the head-first design this replaced: the same 256 MiB buys a
completely different observation now that it is SPREAD (see `plan_windows`)
instead of spent on the title's first 27 seconds.

## `PROMOTE_MIN_DISPLAY_SETS`

How many display sets a SAMPLED run must have seen on a track before "all of
them were forced" may be asserted as a verdict.

A sampled run sees a fraction of a track, so a forced verdict from it is an
absence claim over that fraction. One display set is not an observation of a
track: on a track that flags some of its sets and not others — measured,
such tracks exist, flagging a quarter of their display sets — catching a
single flagged set and calling the track forced is a user-visible mistake (a
full dialogue track that players then force on screen). Two is a low bar,
but it removes the single-hit case that dominates the risk. A run that read
every extent end to end is not sampling and is not subject to this.

## `WINDOW_SECTORS`

Sized against MEASURED subtitle density, not guessed: across a sample of
feature titles a full dialogue track carries a display set every ~30-50 MB
of clip, so a 32 MiB window is about even money on its own and the plan's
eight of them make an observation near-certain. Halving the window and
doubling the count buys the same expected number of observations for the
same bytes — but twice the seeks, and a measured 6x wall-clock penalty on a
source whose read batching collapses after every jump. Fewer, longer
windows.

## `MIN_WINDOW_SECTORS`

A window smaller than this is too short to be likely to contain a display
set at all, so it would spend drive time to learn nothing; a title
fragmented into so many extents that its per-extent share falls below the
floor instead samples fewer extents (the global budget stops the run) rather
than sampling all of them uselessly.

## `MAX_WINDOWS_PER_EXTENT`

Past this, extra windows buy no extra expected observations for the same
bytes (the expectation depends on total bytes read, not on how they are cut
up) and cost another seek each.

## `SampleWindow`

`offset` sectors from the extent's `start_lba`, `len` sectors long. Both are
whole numbers of AACS aligned units, so every read inside the window stays
on the unit grid the decrypting source demands.

## `plan_windows`

The core of the redesign. The forced predicate is ASYMMETRIC: a track is
forced iff EVERY display set carries `forced_on_flag`, so

  * ONE non-forced display set DISPROVES forced, permanently — and the
    tracks that need disproving are the big ones (full dialogue tracks:
    measured shape, one to two thousand display sets spread over the whole
    feature);
  * PROVING forced needs to see the whole track — but a genuine forced
    track is tiny (measured shape: tens of display sets, well under a
    megabyte).

So the expensive-to-prove case is the cheap-to-read one, and the case that
dominates the budget is disproved by a single hit anywhere in the title. A
head-first prefix is therefore the worst possible allocation: it reads the
start of everything, where a feature has no subtitles at all (measured: the
first display set lands well past the first 256 MiB), so it disproves
nothing and observes nothing. Spreading the SAME budget over the extent gives
every window an independent chance of landing on a display set.

The plan is a pure function of `(sector_count, share)` — no clock, no
randomness, no dependence on what has been read so far — so two runs over
the same extent with the same share read the same bytes, which is what makes
the per-extent memo (see `ForcedProbeCache`) reproducible rather than a
snapshot of one run's timing.

## `TrackEvidence`

The two monotone facts a `ForcedTracker` accumulates about one probed
extent, and nothing else.

Keeping the EVIDENCE (rather than a composed forced/not-forced verdict) is
what makes per-extent memoisation sound: both `non_forced`/`forced_seen`
fields only ever go from `false` to `true` as more data is seen, so a
title's verdict is the field-wise OR over its extents, in any order, with no
dependence on how the extents were grouped into playlists.

## `CachedEvidence`

One extent's memoised evidence for one track, WITH the coverage it rests on.

The coverage is the whole point. Evidence from a sampled (or budget-cut)
read describes the sectors that were actually fed to the demuxer, not the
extent — memoising it under the extent's full key and replaying it to
another playlist asserts a completeness the read never had. Recording
`covered` alongside the evidence's own `sampled` flag lets a later run
decide whether the entry answers ITS question or whether the extent has to
be read again.

`CachedEvidence::answers`: `non_forced` is POSITIVE evidence (a non-forced
display set was seen on the wire) and settles the track outright, so its
coverage is irrelevant. Everything else is an ABSENCE claim, and an absence
is only worth what was looked at: honour it only when at least as much of
the extent was covered as this run intended to cover.

## `ForcedProbeCache`

Memoises probe results across titles, keyed PER PHYSICAL EXTENT and per PGS
track — `(start_lba, sector_count, pid)`.

Many playlists on one disc reference the same clips (main feature, play-all,
seamless-branch variants) but rarely with byte-identical extent LISTS: 00800
= `[A, B]`, 00801 = `[A]`, 00802 = `[B]` are three different lists over two
clips. Keying on the whole list de-duplicated only exactly-identical
playlists and re-read every shared clip once per list — up to
`PROBE_BUDGET_SECTORS` (256 MiB) of optical-drive time each. Per-extent
keying reads each physical extent at most once per disc, and per-track
keying means a playlist that declares MORE PGS tracks over the same extents
still probes the extra ones instead of silently taking a verdict map that
has no entry for them.

Only extents whose read reached a DESIGNED stop are memoised (see
`probe_and_set_forced`), so one cancellation or read fault is never frozen
in as an extent's answer — and each entry carries the COVERAGE behind it
(see `CachedEvidence`), so a sampled observation is never replayed as if the
whole extent had been read.

## `StopReason`

Why the read loop stopped — which decides whether the observations it
accumulated may be applied as an authoritative verdict.

The distinction matters because the two kinds of per-track verdict rest on
opposite kinds of evidence:

  * "not forced" is POSITIVE evidence — a non-forced display set was
    actually seen on the wire. Nothing read later can retract it, so it is
    sound no matter how the loop stopped.
  * "forced" is an ABSENCE claim — display sets were seen and none of them
    was non-forced. It is only sound if the read got far enough for that
    absence to mean something. On an arbitrarily truncated prefix it does
    not.

`Budget`: a DESIGNED stop, not a failure — the natural exit never fires for
a genuinely forced track, so the budget exists precisely so that a forced
verdict can be accepted from a bounded prefix. A forced track's display
sets appear throughout the title, so the prefix is representative — treating
this as inconclusive would disable forced detection outright, the very
thing the budget was added to enable.

`Halted`: operator cancellation. The bytes read were read correctly, but the
cut-off point is arbitrary — cancellation can land after a single chunk (or,
as with an already-cancelled halt, after none at all). Epistemically that is
the same arbitrary prefix as a read fault, so an absence claim from it is
not trustworthy.

## `verdicts`

Compose the per-track verdicts a run is ENTITLED to assert from the
evidence it gathered. A track absent from the result keeps its
vendor-derived flag.

Four gates, all PER TRACK, because the evidence is per track:
  * `observed` — saw no display set at all, so nothing is known. (Never
    assert "not forced" from having seen nothing.)
  * on a truncated run, `non_forced` — the track saw an actual non-forced
    display set, which no further reading could retract, so that verdict
    stands even though the run was cut short. A track that merely hadn't
    YET seen a non-forced set is exactly the claim the truncation
    invalidates, so it is dropped and keeps the vendor flag.
  * for the NOT-FORCED verdict, `crate::mux::codec::pgs::demotable` — the
    verdict may be contradicting a vendor label, and "no display set
    carried `forced_on_flag`" says nothing at all on a disc whose authoring
    never sets that flag. Discs like that exist (measured: not one track on
    the disc carries it), and without this gate the probe would strip the
    correct forced label off every track on every one of them.
  * for the FORCED verdict, a minimum display-set count
    (`PROMOTE_MIN_DISPLAY_SETS`) unless the read was complete — over a
    SAMPLE of a mixed track, one flagged set alone is not enough to promote
    the whole track to forced.

## Tests: fixtures and mutation-guard rationale

`TsReader` (fixed BD-TS byte stream, served once then EOF): sector-granular,
like every real `SectorSource` — a read served from the payload's short tail
zero-pads to the sector boundary and reports whole sectors. (The probe
accounts in SECTORS, so a source that returned a sub-sector byte count could
never advance.)

`ts_stream`: the follower carries the SAME display set deliberately. It used
to be a hardcoded NON-forced one, which was invisible only because the probe
threw the last PES of a run away; now that the run's tail is drained, a
contradicting filler would smuggle an observation the fixture never meant to
make.

`PartialTsReader`: serves a fixed BD-TS byte stream, then either fails or
runs on with zeros. Models the two truncated-run shapes: real content
observed, then the read abandoned mid-title, versus real content observed
and then a designed stop at the budget.

`overlapping_extent_lists_read_each_clip_once`: MEASURED — overlapping-but-
not-identical extent lists must not re-read the shared clips. A disc's
playlists share clips without sharing whole extent LISTS (00800 = [X, Y],
00801 = [X], 00802 = [Y]), and keying the cache on the whole list
de-duplicated only exactly-identical playlists: each of the three lists
missed, so clip X was read twice and Y twice — up to `PROBE_BUDGET_SECTORS`
(256 MiB) of optical-drive time per miss.

`extra_pgs_track_over_known_extents_is_still_probed`: a later playlist that
declares MORE PGS tracks over the SAME extents must still probe the extra
track. With the cache keyed on the extent list alone, the verdict map it hit
had no entry for the new PID, so that track was never probed and silently
kept its vendor-label flag — `info` then reported a different forced flag
for it depending purely on playlist ordering.

`probe_reads_stay_on_aacs_unit_boundaries`: every probe read must begin on
an AACS aligned-unit boundary measured from the extent's own base, and the
probe must declare that base to the source. A `DecryptingSectorSource`
holding AACS keys rejects any other read outright (`DecryptFailed`) — and
with a 1024-sector chunk (`1024 % 3 == 1`) every read after the first was
misaligned, so content-based forced detection was unreachable past the first
chunk of an encrypted disc, silently.

`short_reads_do_not_skip_sectors`: a short-but-nonzero read must advance by
what was READ, not by what was requested. Advancing by the request skipped
the unread tail of every chunk — silently, with `StopReason` still
`Exhausted`, so the absence-based forced verdict was asserted (and
memoised) over sectors nobody read.

`short_reads_stay_on_aacs_unit_boundaries`: a short read must not break the
aligned-unit invariant the chunk size exists to hold. `CHUNK_SECTORS` is a
multiple of `ALIGNED_UNIT_SECTORS` so that every read BEGINS on an AACS
aligned-unit boundary measured from the extent base; a source that serves
fewer sectors than requested (a 64-sector prefetch batch: `64 % 3 == 1`)
used to advance `lba` by that raw count, so every subsequent read of the
extent was off the unit grid. `DecryptingSectorSource` rejects those before
reading (`DecryptFailed`) → `ReadFailed` → `absence_is_conclusive()` false →
no verdict asserted and nothing memoised, i.e. content-based forced
detection silently degraded to the vendor label on precisely the encrypted
discs it was fixed for.

`a_source_below_one_aligned_unit_stops_instead_of_spinning`: a source that
can never yield a whole aligned unit cannot be advanced past without
leaving the unit grid — so the loop retries a bounded number of times and
then stops. It must NOT spin: the test simply completing is the assertion,
plus a bounded read count and an inconclusive (uncached,
vendor-flag-preserving) outcome.

`non_pgs_subtitle_codec_is_excluded_from_the_probe`: mutation guard for the
`sub.codec == Codec::Pgs` match guard (probe's PID collection) — only PGS
subtitle tracks are ever probed by content; DVD VobSub forced comes from the
IFO/vendor path, never from sniffing PGS segments over non-PGS bytes. If the
guard were dropped, a non-PGS subtitle stream would be treated as a PGS PID
and the reader would be touched even though there is nothing PGS to probe.

`stalled_retries_stop_at_exactly_the_limit`: mutation guard for `stalled >
STALL_RETRY_LIMIT` — exactly `STALL_RETRY_LIMIT` retries are allowed
(`STALL_RETRY_LIMIT + 1` total read attempts) before the stalled run gives
up. Weakening the comparison to `==` or `>=` still stops the spin (so a
`<=` bound alone does not catch it), but one retry early — after
`STALL_RETRY_LIMIT` attempts instead of `STALL_RETRY_LIMIT + 1`.

`filler_packets`: `STALL_RETRY_LIMIT` padding TS packets (sync byte only,
PID 0 → `adaptation == 0` → discarded harmlessly by the demuxer) so the real
display set lands at a byte offset that survives a correct `got *
SECTOR_BYTES` feed length but is cut off by a mutated `got + SECTOR_BYTES`.

`feed_uses_the_full_read_length_not_a_truncated_one`: mutation guard for
`got as usize * SECTOR_BYTES` (the feed-length computation on a fully-served
chunk) — a 3-sector read must hand the WHOLE 6144-byte chunk to the
demuxer. Padding pushes the real display set to byte 4032 — past `got +
SECTOR_BYTES` (2051) but inside `got * SECTOR_BYTES` (6144) — so a mutated
addition would silently drop it from the feed and the run would never
observe it.

`RealThenZerosReader`: like `PartialTsReader`'s `ThenWhat::Zeros`, but also
counts every sector requested (not just what one extent's read attempted),
so a test can measure how much of a SECOND, effectively infinite extent
actually got read.

`carried_non_forced_evidence_stops_reading_a_content_free_extent`: mutation
guard for the `||` in the early-exit check ("every track has already shown
a non-forced set — counting evidence CARRIED IN from other extents") —
non-forced evidence carried in from a prior extent must stop reading a later
extent immediately, even though that later extent's OWN fresh tracker has
not itself observed anything. Weakening `||` to `&&` requires local
confirmation too, so a huge trailing extent with no PGS content of its own
would be read all the way to the sector budget instead of one chunk.

`TrackShape`: `first_sector` and `period_sectors` must be multiples of
`ALIGNED_UNIT_SECTORS` — a BD-TS packet is 192 bytes and the demuxer works
on that grid from the start of each feed, so only every third sector (3 *
2048 = 32 * 192) begins on the grid.

`SyntheticClipReader`: a feature-length clip — zeros everywhere except
where a `TrackShape` puts a display set. Serves any LBA asked for (unlike
`TsReader`, which must be read in order) — which is exactly what a sampling
probe has to be tested against.

`a_demoted_track_stops_calling_itself_forced`: spec — a verdict that
DEMOTES a track clears a `Forced` qualifier with it. The two fields are one
fact rendered for two consumers — the muxer writes Matroska `FlagForced`
from `forced`, the JSON metadata sidecar writes its qualifier string from
`qualifier` — so leaving the qualifier behind publishes a track that calls
itself forced next to a header that says it is not. Mutation: delete the
qualifier assignment in `apply_verdicts` — the track comes out `forced ==
false, qualifier == Forced`.

`a_demoted_track_keeps_a_qualifier_that_is_not_a_forced_claim`: spec — a
qualifier that is not a forced claim is not the probe's to touch. `Sdh`
says something about the track's content that a forced-narrative verdict
neither confirms nor refutes. Mutation: clear the qualifier unconditionally
on demotion — the SDH marking is lost.

`subtitles_beyond_the_old_head_budget_are_observed`: THE headline fix. A
feature's subtitles do not start at the top of the title — measured, they
begin well past the first 256 MiB — so a head-first budget read the opening
minute of black and logos, observed NOTHING, and contributed nothing to the
verdict on any disc long enough to matter. Spreading the SAME budget over
the extent puts windows where the subtitles are.

`the_budget_is_spread_across_the_extent`: the allocation, not just the
total — the budget must be spread across the extent instead of poured into
its head. The track here is forced throughout, so it never settles and the
run spends its whole budget — which is precisely the run whose ALLOCATION
matters. (A track disproven by its first display set stops the run early,
by design; that is the per-track exit, tested separately.)

`a_thin_sample_is_not_replayed_to_a_playlist_that_would_read_more`: the
memoisation hazard. A sampled read covers a FRACTION of an extent, so its
evidence is a statement about that fraction — but it used to be filed under
the extent's full key and replayed to every other playlist sharing the
clip, including playlists that would have read far more of it. An absence
claim ("no non-forced display set here") inherited that way asserts a
completeness the read never had.

`a_disc_that_never_sets_the_forced_flag_cannot_demote_anything`: the case
the guard exists for. On a disc whose authoring never sets `forced_on_flag`
— measured: not one track on the disc carries it — the absence of the flag
says nothing whatsoever about any track. Demoting on it would strip the
correct forced label off every forced track on every such disc.

`a_forced_shaped_track_keeps_its_label_on_a_disc_that_uses_the_flag`: the
other side of the guard — a track with the SHAPE of a forced track
(measured: tens of display sets against a full track's thousands) keeps its
label even on a disc that uses the flag. The flag being in use elsewhere
does not oblige every forced track to carry it.

`cached_evidence_answers_only_what_its_coverage_supports`: coverage is what
makes a memo replayable. An entry from a thin sample must not answer a
question that needs a thorough one — but positive evidence (a non-forced
display set was SEEN) settles the track whatever the coverage, and a
complete read answers everything.

`re_reading_one_extent_merges_its_memo_instead_of_doubling_it`: a playlist
may list the SAME clip twice. The second read must merge into the extent's
memo, not double it — `displays` feeds the demotion shape test, and
counting the same display sets twice would inflate a track towards being
demotable on evidence that was only read once.

`the_last_display_set_of_a_run_is_not_thrown_away`: the tail of a sampled
run must not be discarded. The demuxer holds the last PES of a run open
waiting for the next PUSI, which — with sampling — lies in another window
or nowhere at all. Unflushed, that is one lost display set per window,
worst at exactly the places the sample is thinnest.

`a_single_window_sample_is_taken_from_the_middle_of_the_extent`: a title
cut into many clips gives each extent a single window's worth of budget.
That window must not sit at the extent's head — sampled at the head, every
clip is read at the same relative position, and for the first clip that
position is the opening of the feature: the one stretch that reliably has
no subtitles in it, which is the whole defect being fixed.

`one_display_set_in_a_sampled_run_does_not_prove_a_track_forced`: promotion
is an absence claim too, and a SAMPLE cannot support it off a single display
set. Measured: tracks exist that carry `forced_on_flag` on a quarter of
their display sets and not on the rest — catch one of the flagged ones and
nothing else, and a full dialogue track gets flagged forced, which players
then force on screen.

`a_complete_read_may_still_promote_from_one_display_set`: a run that read
every extent end to end has no unread gap to hide a non-forced set in, so a
genuine single-sign forced track (measured: they exist, one display set for
the whole feature) is still promoted. Pinned by
`the_last_display_set_of_a_run_is_not_thrown_away`, which is exactly that
case; this asserts the two rules do not collide.
