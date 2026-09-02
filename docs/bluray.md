# src/disc/bluray.rs — extended notes

Overflow prose for comments that were trimmed to the internal 3-line cap in
`src/disc/bluray.rs`. Each section is pointed to by a `// See docs/bluray.md
— <topic>` comment at the corresponding site.

## CLIP_STREAM_EXTS

Stream-file extensions probed for a BD-family playlist clip, in priority
order. A clip is normally `.m2ts`; AACS 2.1 (FMTS) discs name the main
feature `.fmts` (an M2TS transport stream plus forensic variant segments)
and 3D discs use `.ssif`. `.m2ts` is tried first, so a normal clip is
unaffected — the fallback only runs when `.m2ts` is absent (exactly when
`file_extents` errors).

Scope: these are all variants that live in `BDMV/STREAM/` and are reached
through an MPLS playlist. HD-DVD's `.evo` does NOT belong here — HD-DVD is
a different tree (`HVDVD_TS/`) with `.XPL` playlists and needs its own
enumerator (a peer to `parse_playlist`), not another extension in this
list.

## scan_bluray_titles

Cancellation: `halt` is polled between playlists and once more after the
loop, and a read that fails with `Error::Halted` — how a live drive
reports a Stop, since `Drive::checked_exec` fails every command once its
flag is set and `Drive::read` preserves the variant — is propagated rather
than swallowed. `Halted` is the only error this returns from the
enumeration itself; an unreadable or unparseable playlist keeps its
best-effort skip.

It has to be an error and not a short title list, for the same reason
spelled out on `Disc::scan_hddvd_titles`: a cancelled enumeration that
returned `Ok` would be indistinguishable from a disc that genuinely holds
fewer titles, and the caller would cache and act on it. Before this, a Stop
pressed mid-scan failed every remaining `.mpls` read in turn, each one
silently skipped, and the scan returned `Ok(truncated)` at rc=0.

## parse_playlist

Sums PlayItem durations; returns `Ok(None)` if the playlist is under 30
seconds (skips menu / clip-info stub playlists), fails to parse, or names a
clip that cannot be resolved. Physical sector extents are pulled from the
UDF allocation descriptors of each referenced `.m2ts` (deduplicated by
clip_id).

`Ok(None)` keeps its two benign meanings (unparseable MPLS, sub-30s
playlist) plus the deliberate "drop this title" outcomes below. `Err` means
the scan is over: today that is only `Error::Halted`, the operator's Stop,
which must not be reported as a disc that simply holds fewer titles.

## Test: parse_playlist_keeps_exactly_30_seconds

At exactly 30s the playlist is kept (`< 30.0` is strict). The fixture is a
fully wired BDMV (`make_bdmv_fs`), not the bare `make_min_fs` this used to
use. `make_min_fs` lays no STREAM/CLIPINF, so the play item's `.clpi` read
now fails and the title drops — which is correct behaviour for an
unresolvable clip but has nothing to do with the 30-second boundary this
test exists to pin. Wiring the clip keeps the test measuring exactly one
thing.

## make_bdmv_fs_ssif

Full BDMV with a real Blu-ray 3D layout: `.ssif` files under
`BDMV/STREAM/SSIF/<clip>.ssif` (note the SSIF subdirectory, unlike
`make_bdmv_fs_ext`) plus a matching `.clpi` in CLIPINF. Resolving the SSIF
is what latches `is_3d = true` in `parse_playlist`.

## Test: parse_playlist_fmts_clip_resolves_extent

AACS 2.1: the feature clip is `00001.fmts`, NOT `.m2ts`. The
`CLIP_STREAM_EXTS` fallback in `parse_playlist` must still resolve the
physical extent — before the fix the hard-coded `.m2ts` path errored,
yielding empty extents (a silent empty rip and 0 encrypted samples for key
resolution). Size still comes from the `.clpi`, which parses regardless.

## Test: parse_playlist_dedups_repeated_clip_extents_and_size

THE 0.31.0 DEDUP PATH. A playlist that references the SAME clip_id from
multiple PlayItems (seamless split / looped segment) must count the
physical extents and packet bytes exactly once — mux reads extents in
order, so a duplicate would mux the A/V twice and inflate `size_bytes`
(bluray.rs: `first_ref = seen_clips.insert(...)` gates both `total_size +=`
and the `extents.push`). Per-PlayItem Clip entries are still recorded for
both.

## Test: parse_playlist_missing_clpi_yields_no_title

A clip whose `.clpi` is missing must yield no title.

This test used to assert the opposite — a title with `size_bytes == 0` and
empty extents — and its docstring quoted the buggy control flow ("bluray.rs
only fetches extents inside the `if let Ok(clpi_data)`") as if it were the
specification. It was blessing the defect: the title's `duration_secs` is
summed from the PlayItems before the clip is resolved, so the returned
title advertised the movie's full runtime while carrying none of its
bytes, and the discarded error meant not one log line said so. That is the
flagship failure class of this crate — a failure that looks like success —
reached through an ordinary missing or scratched CLIPINF file.

The correct behaviour is the same as for a clip whose extents cannot be
resolved (see `parse_playlist_unreadable_clip_icb_yields_no_title`): drop
the title and log the read's own error code.

That last clause is now asserted, not merely asked for. The site's comment
insists on `e.code()` because a missing `.clpi` (E6003), a scratched one
(E6000) and a malformed one (E6002) are different populations, but nothing
checked it: putting a literal back compiled and passed. Mutation: `"E6017"`
(or any fixed code) in place of `"E{}", e.code()` fails here, and so does
dropping the warn entirely — a silent drop is the same invisible title
loss this test was written for, one step later.

## Test: parse_playlist_unrecorded_extent_yields_no_title

A clip stream whose ICB declares an UNRECORDED (ECMA-167 4/14.14.1.1
type-1) extent must not yield a title at all.

The extent is allocated to the file but was never written, so the file's
content there is zeros while the media holds whatever was left at those
sectors. Neither answer a `(lba, sector_count)` read plan can give is true
— reading it splices undefined sectors into the rip as content, dropping
it slides every later extent's byte space — so the title is refused rather
than mis-ripped. This fixture is the shape a crafted disc uses to get such
a range into a title's extent list.

(The `sectors > 0 && lba > 0` filter below the resolver stays as defence in
depth; a zero-length AD is only reachable as an unrecorded descriptor,
since a zero-length TYPE 0 one terminates the AD list.)

## Test: parse_playlist_unreadable_clip_icb_yields_no_title

An unrecorded extent was never the only way a clip fails to resolve.

RED BEFORE GREEN: this fixture gives the .m2ts an ICB whose descriptor tag
is neither 261 nor 266, so `file_extents` returns `DiscRead` — the same
variant a scratched sector under a real clip's ICB produces, which is the
ordinary way this happens on real media. Before the fix only
`UdfUnrecordedExtent` set the drop flag, so this fell through to the "file
absent" path and `parse_playlist` returned a title: full declared duration
from the play item, `total_size` already counted from the .clpi, and zero
extents — a movie advertising its runtime with the content missing, and
not one log line. The clip must drop the title exactly as an unrecorded
extent does.

## Test: parse_playlist_logs_a_non_absence_ssif_failure_the_m2ts_fallback_hid

A non-absence SSIF failure that the `.m2ts` fallback then papers over must
be logged, with its own code.

`unresolved` had exactly one reader — `if let (None, Some(code))` — so a
code recorded for `/BDMV/STREAM/SSIF/<clip>.ssif` was thrown away whenever
the fallback succeeded. The disc here IS a 3D disc: the SSIF is present and
carries both eyes, and only its ICB is unreadable (a scratched sector, the
ordinary way this happens). The title shipped base-view 2D at rc=0 with
`is_3d` false and not one log line, so the operator's rip is silently
missing the dependent view and the journal cannot tell this disc apart
from one that was only ever 2D.

The title is deliberately still returned — the base view resolved, so
refusing would trade a degraded rip for no rip. The defect being fixed is
the silence, not the fallback.

Mutations this catches: deleting the new `(Some(_), Some(code))` arm, or
restoring the `if let (None, Some(code))` shape, leaves no event to find;
logging a fixed code instead of `e.code()` fails the code assertion;
making the arm `return Ok(None)` fails the title assertion.

## Test: parse_playlist_id_falls_back_to_zero_when_suffix_is_not_mpls

A filename that is long enough (>= 5 bytes) but does NOT end in ".mpls"
must NOT have its last 5 bytes stripped — the whole string is handed to
the numeric parse instead, which fails and falls back to playlist_id 0
(bluray.rs `filename.len() >= 5 && filename[len-5..].eq_ignore_ascii_case(".mpls")`).

## Test: scan_bluray_titles_skips_non_mpls_extension_file

A non-directory PLAYLIST entry whose name does NOT end in ".mpls" must be
skipped even though its content parses as a perfectly good (long) MPLS
playlist — extension gating, not content sniffing, decides eligibility
(bluray.rs `!entry.is_dir && entry.name...ends_with(".mpls")`).

## HaltingReader

A `SectorSource` that fails every read in `halt_range` with
`Error::Halted` — exactly how a live drive behaves once the operator
presses Stop: `Drive::checked_exec` fails every SCSI command with `Halted`
from then on, and `Drive::read` deliberately preserves the variant. Reads
outside the range still succeed, so a test can aim the cancel at one
structure and leave the scan far enough along to have something to
truncate.

## Test: halted_playlist_read_is_not_reported_as_a_shorter_disc

A Stop on a live drive never touches `ScanOptions::halt`: `Drive` has its
own flag and `checked_exec` fails every SCSI command with `Error::Halted`
once it is set. The Blu-ray enumerator must not swallow that into a
successful scan.

RED BEFORE GREEN: with the propagation reverted this returned
`Ok([00800])` — the `if let Ok(mpls_data)` skipped the cancelled read of
00801.mpls, the loop ended, and a half-enumerated disc came back at
success. One title from a two-title disc is indistinguishable from a disc
that genuinely holds one title, and the caller caches and rips from it.

The halt lands on the second playlist deliberately: it is the last
iteration, so nothing after it would poll a flag — only propagating the
read's own error catches it.

## Test: halted_clpi_read_is_not_accounted_as_an_unresolvable_clip

The same cancel landing on a `.clpi` read must not be classified as an
unresolvable clip either.

RED BEFORE GREEN: with the `Err(Error::Halted)` arm removed from the
CLIPINF match, the cancel fell into the generic "clip could not be
resolved" arm, which logs a disc-defect code and drops the title —
accounting an operator Stop as a scratched disc, and (in the scan loop)
dropping every remaining playlist in turn for a truncated `Ok`. With the
fix the cancel is propagated with its own variant intact.

## Test: halted_extent_resolve_is_not_a_title_missing_its_clip

And the same cancel landing on `file_extents` — the clip's ICB, not its
CLIPINF — must propagate too.

RED BEFORE GREEN: `note` used to exempt `Halted` from the unresolved
classification (correctly — a cancel is not an authoring hole) but had no
way to propagate it, so the resolver simply produced no extents and
`parse_playlist` returned a title with the clip's runtime counted, its
`size_bytes` counted from the .clpi, and zero bytes behind it. Measured
with the propagation reverted: `Ok(Some((768000, [])))` — the flagship
defect shape, wearing a cancel.

## Test: read_meta_title_ignores_non_xml_file_regardless_of_content

A non-.xml file must be ignored even if its content looks like a valid
meta XML (contains a `<di:name>`) — extension gating, not content
sniffing, decides eligibility (bluray.rs `!e.is_dir &&
e.name...ends_with(".xml")`).
