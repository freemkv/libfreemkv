# `src/disc/extract.rs` — decrypted file-tree extraction

## Relation to the ISO sweep/patch recovery passes

`extract_tree` is a sibling of the disc→ISO sector dump (the sweep/patch
recovery passes, which now live in the `freemkv-engine` crate), specialized
to write **per file** rather than a whole image, applying decryption on the
way out, and **without** any multipass / recovery orchestration — it is
1-shot, decrypt-only.

No new error codes are introduced: it reuses the existing `Error` set
(`error.rs` is numeric-only, no English).

## `extract_tree` contract details

`reader` is consumed for content reads (it is moved into a
`DecryptingSectorSource`); the structure + extent metadata are read first
with the raw reader. `dest` receives the tree STRAIGHT IN (no auto-named
subfolder). The caller must have run the pre-flight decrypt gate
(`Disc::ensure_decryptable`); this method resolves per-VTS CSS keys itself
for DVD.

Bad sectors become recorded zero-filled holes in the affected file (the run
does not abort); undecryptable units are counted as loss. Files are written
`<name>.partial` and renamed on success, so an interrupted run never leaves
a half-written file that looks complete.

## `resolve_vts_key` — why a failed crack must be a hard error

FALLIBLE, because the three outcomes of a crack are not two.
[`crate::css::CrackOutcome`] exists precisely to separate "no scrambled
sector was seen, so there is nothing to decrypt" from "scrambled sectors
were seen and no key came out", and its doc says callers MUST surface the
second as a hard error. An earlier version of this function used the
`Option`-returning `crack_key`, which collapses both into `None`, and then
fell back to the disc-wide key — descrambling this VTS with ANOTHER VTS's
title key. The hazard is described in the ordering note in the function
body, which was written about the same fallback; ordering makes the crack
far more likely to succeed, but it cannot make a failed crack safe.

## `Borrowed` — why a borrowing `SectorSource` wrapper

Lets the decrypting decorator "own" an inner source for its lifetime while
the caller keeps the underlying `&mut dyn SectorSource` (the decorator is a
`DecryptingSectorSource<S>` generic over `S`, so it does NOT require a
`'static` boxed inner — unlike the mux highway, which takes the reader by
value). The decorator is dropped before `extract_tree` returns, so the
borrow never escapes.

## `whole_unit_batch` — the trailing-partial rule

`remaining` is the sectors left in the current extent; the batch is capped
at `READ_BATCH_SECTORS` and rounded DOWN to a whole number of 3-sector units
UNLESS it is the extent's final (possibly short) tail — the tail always
begins on a unit boundary, so a 1-2 sector partial there is handled by
`decrypt_sectors`' trailing-partial contract. Used by `extract_one_file` so
this rounding rule lives in exactly one place.

## `available_space` (windows) — why the free-space gate matters

Windows has no `statvfs`. This once returned `None` unconditionally, which
does not merely skip a test — it skipped the free-space GATE, so a Windows
user extracting a disc to a full volume got a confusing failure part-way
through instead of a clear refusal up front. libfreemkv ships a Windows
GUI, so that is the platform where the friendly error matters most.
`FreeBytesAvailableToCaller` (not `TotalNumberOfFreeBytes`) is used because
it accounts for per-user quotas, matching what `statvfs`'s `f_bavail` gives
on the unix side.

## `sanitize_component` — why reserved names are substituted, not rejected

A name whose base matches a Windows reserved device name (`NUL`, `CON`,
`COM1`..) is **substituted** (prefixed with `_`) rather than rejected — a
Linux-authored disc may legally carry such a file and a single reserved
name must not abort the whole tree walk. An empty result after stripping
is an error.

## `names_differing_only_by_case_are_a_collision` — why this must be an error

Two disc names differing only by CASE are one host file on macOS APFS and
Windows NTFS, both case-insensitive by default. Keyed by the case-preserving
path, the collision map would see two entries, raise nothing, and the
second file extracted would overwrite the first — while both `PlannedFile`s
report `complete: true`. Silent data loss reported as a clean extract is
the one outcome this crate must never produce, so the host-equivalence key
has to model the host's namespace, not the disc's.

## `a_files_partial_path_colliding_with_another_files_final_name_is_an_error`

A disc carrying both `X` and `X.partial` plans two distinct final names, so
nothing collides in the final-name check — but extracting `X` writes
through `X.partial`, the same host path the other file owns. Whichever
lands second truncates the other, and both entries would still be reported
complete without this guard.

## `multi_extent_aacs_anchors_unit_base_per_extent` — the alignment bug

Regression (rc.6 audit, finding #449): a MULTI-EXTENT AACS file must
re-anchor the unit-alignment base PER extent, not once at the first extent.
The two extents in this test start at absolute LBAs whose difference is NOT
a multiple of 3 (`PART_START+5000` vs `PART_START+5004` — a 4-sector delta).
With a single first-extent unit base the second extent's first read is off
the unit grid (offset 4, `4 % 3 == 1`) → `is_unit_aligned` fails →
`DecryptFailed` → the whole extent becomes a zero-filled (false) hole. With
per-extent anchoring both extents read clean: `bytes_unreadable == 0` and
the file content survives. The fixture content is clear (no TS syncs) so
the decrypt step restores each unit verbatim — this isolates the GATE from
the cipher math.

## `extract_tree_zero_fills_an_unrecorded_extent_instead_of_reading_it`

An ECMA-167 4/14.14.1.1 type-1 extent is ALLOCATED BUT NOT RECORDED: the
space belongs to the file and occupies its byte range, but nothing was ever
written there, and the standard defines its contents as zeros.
`read_icb_extents` keeps the flag (`IcbExtent::recorded`) and
`read_file_limited` honours it by emitting zeros WITHOUT touching the
media. The tree extractor reads the same ICBs and must agree: reading those
sectors returns whatever the media happens to hold there — on an AACS disc,
ciphertext that decrypts to noise — and writes it into the extracted file
as if the disc had recorded it.

## `per_extent_base_is_aligned_first_extent_base_is_not`

Focused alignment-computation check underpinning the per-extent fix: when
each extent anchors its OWN start as the unit base, the extent's own batch
starts are always unit-aligned; anchoring a later extent against the FIRST
extent's base mis-aligns whenever the extents' starts differ by a
non-multiple of 3 sectors. This is the exact arithmetic the decrypt-on-read
gate (`aacs::content::is_unit_aligned`) performs.

## `css_two_vts_groups_do_not_cross_contaminate_keys`

Regression: the per-VTS key crack in `resolve_vts_key` must gather ONLY
this VTS's own title-VOB extents. Two VTS groups here carry DISTINCT
scrambling keys; if the group filter (`vts_group_of(..) == Some(vts) &&
is_title_vob(..)`) is loosened (`!=`, or `&&` -> `||`), one VTS's resolve
gathers the OTHER VTS's extents, cracks the wrong key, and that VTS's own
content silently fails to descramble to the expected plaintext.

## `a_scrambled_vts_that_cannot_be_cracked_fails_instead_of_borrowing_a_key`

A VTS that IS scrambled but whose key could not be recovered must FAIL, not
borrow another VTS's key. `crack_key` returns `Option`, which collapses "no
scrambled sector was seen" with "scrambled sectors were seen and no key
came out"; the fallback then descrambled this VTS under the disc-wide key
and `extract_tree` reported `complete = true` at exit 0, because no read
had failed. `CrackOutcome` exists to keep those two apart, and every
sibling path in the crate already honours it. The fixture: VTS_01 is
crackable, VTS_02 is scrambled with NO periodic crib, so its crack
genuinely fails.
