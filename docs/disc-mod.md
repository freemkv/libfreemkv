# src/disc/mod.rs — internal notes

Overflow rationale for internal (non-`pub`) items in `src/disc/mod.rs`,
relocated here by the comment-guard so the in-file comment can stay short.
Each section is pointed to by a one-line `// See docs/disc-mod.md — <topic>`
comment at the corresponding call site.

## aacs_dir_present

THE ONE definition of "is this disc structurally AACS-encrypted". Both
`Disc::identify` (fast path, filesystem only) and `Disc::scan_with` (full
scan) need this, and both used to spell out the same two `find_dir` calls by
hand. They agreed today; nothing made them agree tomorrow — adding a third
AACS location, or excluding an empty placeholder directory, to one copy and
not the other would silently desync the fast identify from the full scan (the
same disc reported encrypted by one and clear by the other).

This is STRUCTURAL, not cryptographic: it says the tree looks like an
encrypted disc, not that any sector actually is. A folder copied verbatim
from a decrypted disc keeps its `AACS/` and answers true here (see
`session::scan_dir`, which corrects for exactly that).

## correct_truehd_channels

Corrects a title's TrueHD audio-stream metadata by probing the first
decrypted access units — channel count, real sample rate, and Atmos detection
in a single major-sync read. The MPLS descriptors declare the BASE layout
(often 5.1, or a container-guessed rate) even for a 7.1/Atmos TrueHD track;
the truth is in the MLP major sync. `reader` must yield DECRYPTED sectors
(the m2ts is AACS-encrypted, so this can only run at mux time, not scan).
Reads a bounded window of the title's first extent.

Corrections, each individually guarded so a malformed field never writes a
wrong header:
- **Channels**: from the presentation channel masks (as before).
- **Sample rate**: from the whitelisted rate nibble; left untouched on an
  unknown rate or no major sync.
- **Atmos**: when a 4th substream is detected AND the stream still carries
  the basic descriptor label, the label is promoted to the Atmos form;
  richer editorial labels (e.g. an existing "Dolby Atmos") are left intact.

## Resolution::pixels

`Unknown` deliberately does NOT fabricate a plausible 1920x1080. This is the
same trap already closed on `AudioChannels::count` (which used to return 6)
and `SampleRate::hz` (48000.0): a plausible wrong answer is indistinguishable
from a real one at every call site, so a caller can forget to check the
variant first — the `json://` sink had already walked into exactly that,
reporting confident dimensions for a stream whose neighbouring `resolution`
field said "unknown".

It also does not return `(0, 0)`: that shape looks like a usable pair, so the
MP4 sink stored it and serialised a 0x0 video track — a structurally complete
file no player can render, written with no error. Before that it returned a
fabricated 1920x1080, which was wrong but at least playable, so the sink had
never needed a guard and the absence of one was invisible. A doc comment
listing which callers are safe cannot hold this: one did not check, and a
third kept its own duplicate `Unknown` test long after the accessor took the
job over. `Option` forces every caller to make its OWN decision: Matroska
omits the optional PixelWidth/PixelHeight elements, the metadata sinks report
the dimensions as absent, and MP4 must refuse per ISO/IEC 14496-12 (width and
height are mandatory in both `tkhd` 8.3.2 and VisualSampleEntry 12.1.3).

## has_probable_video

The exposed `streams` come from the title's FIRST PlayItem only, so a
feature whose first PlayItem is a non-video bumper reports
`has_video() == false` and would be wrongly disqualified (audit R6). This
predicate is deliberately MORE permissive than `has_video` — it only ever
ADMITS a title the stricter gate would reject — so it cannot reopen the
decoy hole: a size-inflated streamless decoy is demoted STRUCTURALLY by
composite detection in `Disc::rank_titles`, not by this gate, while it still
rejects tiny streamless menu/metadata playlists. The size fallback requires
at least one clip, so a clip-less pure-metadata decoy (which cannot be a
real bumper-lead feature — that has the feature clip) is still rejected.

## image_crack_extents

The extents an image-time CSS crack scans, in the crate's CANONICAL order:
the main feature's own extents, in natural playback order. Both halves are
deferred to logic the crate already owns rather than re-derived here:

- WHICH title — `scan_with` has already sorted `titles` with
  `Self::canonical_title_order`, so the canonical main feature is the first
  title that actually has extents. No local pick.
- WHAT ORDER — playback order, i.e. the extent vector untouched, exactly as
  `Self::decrypt_keys_for_title` hands `&title.extents` to
  `css::crack_key_outcome`.

**Why this function exists (do not let the copy grow back):** `Disc::scan_image`
used to re-implement both halves inline — it picked the title with the
largest total sector count, then re-sorted that title's extents
LARGEST-CELL-FIRST before handing them to `crack_key_outcome`. Both had
drifted from the canonical rules:

- Largest-cell-first is the 1.5.1 garbage bug. A CSS DVD's biggest cell opens
  with a long CLEAR run, and the crack's 50_000-sector budget is shared
  across the whole extent list, so starting there can exhaust the budget
  without ever reaching a scrambled sector — the crack reports `Unencrypted`
  and the mux emits scrambled MPEG as plaintext. Playback order reaches the
  scrambled feature body after only the small clear front matter that
  precedes it on a real disc (CSS: the title key is recovered from the
  scrambled data itself, so the scan must actually MEET scrambled data).
- The sector-count pick ignores `canonical_title_order`'s capacity gate, so
  it selects the oversize "play-all" composite (whose declared cells
  double-count data shared with other playlists) instead of the real feature
  — i.e. a DIFFERENT title, and on a multi-VTS disc a different VTS, whose
  CSS title key does not descramble the feature at all.

The result: the same disc cracked as an ISO could disagree with the same
disc cracked from the drive, which is precisely what the duplicate was free
to do. Keep the derivation here, shared, so it cannot recur.

## read_aacs_inputs_from_reader

Reads a disc's AACS key-input files from a sector source: returns
`(Unit_Key_RO.inf, MKB)` raw bytes. Shared body for `Disc::read_aacs_inputs`
(ISO) and `Disc::read_aacs_inputs_from_drive` (live drive). Prefers MKB_RO,
falls back to MKB_RW, then TRIMS to the real record length. Both files are
allocated to a fixed ~128 MiB and zero-padded, so reading either ships up to
~124 MiB of nothing — trim to the record stream so callers send/store a few
MB, not 128 MiB.

## read_aacs_version

AACS major version (`crate::aacs::mkb::AACS_MAJOR_BD` /
`crate::aacs::mkb::AACS_MAJOR_UHD`) from the content certificate. Drives the
`Unit_Key_RO.inf` parse stride (48-byte V10 vs 64-byte V20/V21), so the
out-of-band key-fetch path parses `enc_title_keys` at the right stride (a
server VUK then derives the correct unit keys).

When no content certificate is readable/parseable, defaults to **UHD (V20,
64-byte stride)** — the conservative choice the pre-1.2.0 fetch path
hardcoded — and logs it: a wrong stride here folds a server VUK against
mis-strided title keys (silent wrong unit keys), so a missing cert must not
quietly pick the V10 stride for a UHD disc.

## read_mkb_content

Reads the AACS MKB's real record stream — NOT its zero padding. `MKB_RO.inf`
/ `MKB_RW.inf` are allocated to a fixed ~128 MiB and zero-padded; the actual
record stream is a few MiB. Reads a bounded prefix, finds the record-stream
length via `crate::aacs::mkb::mkb_content_len` and returns exactly that,
growing the prefix if the records run past it. This avoids reading 100+ MiB
of padding on every scan AND avoids the `read_file` `MAX_FILE_BYTES` cap that
(since 0.31.0) rejected the padded 128 MiB MKB outright — which made
`read_aacs_inputs` fail and autorip report "could not read this disc's key
files" without ever contacting the keyserver.

## CANONICAL_TITLE_ORDER_KEYS

**Why not just sort by duration descending?** Branching UHDs (and some BD
authoring) ship a "play-all" virtual playlist that references the same
source clips multiple times for seamless alternate-angle / alternate-ending
playback. Those playlists report an inflated `duration_secs` (often 4+
hours) and an inflated `size_bytes` greater than the disc's physical
capacity. Example seen in the wild — *The Amateur (2025)* UHD, 58.5 GB
BD-100 disc:

| Title | Playlist   | Duration | Size    | Clips |
|-------|------------|----------|---------|-------|
|   1   | 00020.mpls | 4h 13m   | 92.4 GB |  253  |
|   2   | 00800.mpls | 2h 02m   | 57.2 GB |    1  |

Title 1's 92.4 GB cannot fit on a 58.5 GB disc unless the same clip data is
referenced multiple times — proof it's a virtual composite. A duration-only
sort would put it at `titles[0]`, so `freemkv -t 1`, `disc.titles.first()`,
and autorip's main-feature picker all grab the 4-hour composite instead of
the 2-hour movie that actually matches TMDB.

**Effect on non-branching discs:** unchanged — the main movie is already the
longest 1-clip title. **Effect on branching UHDs:** the virtual play-all
playlist is pushed to the back, the actual movie surfaces at index 0.

Defined immediately beside the comparator, so a diagnostic can NAME the
ordering instead of restating it. The `freemkv::diag` main-feature decision
row used to carry its own hand-written copy of this list, and it drifted: it
still advertised a `fewest-clips` key long after the comparator replaced
clip-count with largest-physical-size, so the `--log-level 3` bug-report log
explained freemkv's top-level pick with a rule freemkv does not apply. Any
change to the keys must change this list in the same edit.

## probe_forced_subtitles_for_bdts_titles

Pulled out of `scan_with` as its own callable predicate: the decision is
otherwise reachable only by driving the full scan pipeline (tree dispatch ->
title parsing -> this loop), so a test could not pin the gate without also
authoring an entire synthetic disc image. Running it against an HD-DVD/DVD
(`MpegPs`) title's extents would demux the wrong container, since only
`BdTs` titles carry PES-wrapped PGS the shared classifier
(`pgs_forced_probe`) understands.

## MAIN_FEATURE_ORDER_KEYS

- `nav-feature`: the title the disc's own HDMV navigation plays as the
  feature, resolved by running First-Play in the `crate::bdnav` VM the way a
  real player would. This is the authoritative, size-independent pick; it is
  video-gated (a mis-resolve cannot select a streamless playlist) and simply
  absent when the VM abstains (BD-J discs, or non-convergence), in which
  case the lower keys decide.
- `authoring-feature`: a title the disc's own menu authoring designates as
  the feature (has video, is not itself a composite, and is within a sane
  fraction of the longest title's duration) outranks everything else — a
  size-independent signal an obfuscation decoy cannot forge.
- `standalone`: a title that is a play-all / wrapper COMPOSITE — its clip
  set properly contains another substantial video title's whole clip set (a
  real UHD title's decoy playlist `00245` = `[bumper][00001 feature
  clip][outro]` case) — is demoted below the standalone title it wraps. This
  is the STRUCTURAL anti-decoy signal; it does not depend on stream counts,
  physical size, or disc capacity, so it survives where those are spoofed or
  unknown.
- `has-video`: a title with no plausible video content (see
  `DiscTitle::has_probable_video`) is demoted below every title that has
  some — a floor that drops streamless menu/metadata playlists.

## rank_titles

This catches both the wrapper decoy (`00245 ⊋ 00001`) and the oversize
play-all — independent of capacity, which the `fits-disc` gate needs and
which is `0`/unknown on drives that fail READ CAPACITY. The corpus
title-selection gate is the backstop across the disc hoard for the known
residual (a real feature with a separate "resume from the middle" branch
playlist reusing most of its clips).

## aligned_unit_keys_validate

Conservative: a sample that is not AACS-scrambled proves nothing, and with
no scrambled sample at all there is nothing to disprove against, so it
returns `true` (accept). It returns `false` when ANY scrambled sample cannot
be restored to clear MPEG-TS by ANY unit key in the set. That covers two
distinct failure shapes: (1) a wholly wrong key (a keydb VK that does not
match this disc) — no sample decrypts; and (2) a *partially* applicable key
set on a multi-CPS-unit disc — the resolved keys cover CPS unit 0 but not
CPS unit 1. Accepting on the first sample that decrypts (the old behaviour)
would commit such a set, after which CPS-unit-1 sectors pass through as raw
encrypted bytes into the ISO/MKV with no error surfaced anywhere. Requiring
every scrambled sample to decrypt rejects the incomplete set so the caller
falls through to the next candidate. Reuses the ecosystem's single
`is_clean` content-clarity predicate and the full (bus + AACS) unit decrypt,
so it agrees with the actual mux decrypt.

## ensure_decryptable

The verdict, in order:
- `raw == true` → `Ok(())`. `--raw` intentionally skips decryption and needs
  no key (the caller wants an encrypted image).
- `self.css_error.is_some()` → `Err(Error::CssNoDiscKey)`. The scan saw
  scrambled CSS sectors but recovered no title key (`self.css` is `None` yet
  the content IS encrypted). Treating `css.is_none()` as "unencrypted" would
  mux scrambled MPEG as plaintext garbage. A DISC-LEVEL verdict
  (`error::is_disc_level_no_key`) — the main feature's crack failed, so
  every title fails the same way and the rip loop must stop rather than
  skip each title in turn.
- AACS-encrypted (`self.aacs.is_some()`) with no usable key
  (`decrypt_keys()` is `None`) → `Err(Error::NoDiscKey { .. })`, naming the
  disc by hash.
- CSS-encrypted (`self.css.is_some()`) with no usable key →
  `Err(Error::CssKeyMissing)`. (The disc-wide `decrypt_keys()` yields
  `Css{..}` whenever `css.is_some()`, so this is defensive; the live
  multi-VTS case is gated by `Self::ensure_decryptable_keys`.)
- otherwise → `Ok(())`. A genuinely unencrypted disc has `None` keys
  legitimately, and a CSS disc whose keyless crack succeeded has a key.

## decrypt_keys_for_title

For a DVD the CSS title key MUST be recovered before descrambling: a
scrambled sector without a recoverable crib cannot self-crack, and CSS
leaves the pack/PES header clear, so a sector left un-descrambled would mux
as a structurally-valid but corrupt PES packet with no loss reported. Two
ways to get it:

- **Fast path** — the scan already cracked a key whose LBA span
  (`crate::css::CssState::crack_span`) covers this title's VTS: reuse it. No
  re-read, and on a live drive no second CSS bus-auth round-trip. CSS title
  keys are per-VTS, so an overlapping span is the same key.
- **Crack** — when up-front detection missed (`self.css == None`) or the
  title lives in a different VTS: crack the key from this title's OWN
  extents in a SINGLE scan, in natural PLAYBACK ORDER (never
  largest-cell-first, which starved the crack in a big cell's clear prefix —
  the 1.5.1 bug). Playback order reaches the scrambled feature body after
  only the small clear front matter (logo / rating card) that precedes it.
  One scan = one CSS-locked early-bail, so a locked title is not
  re-hammered per cell against a live drive (hard rule #2); its 50k-sector
  budget is the same accepted bound the disc-wide scan uses.

Outcomes (`batch_sectors` sizes the crack's batched reads):
- `(Css{title_key}, false)` — descramble with the recovered/reused key.
- `(None, true)` — a genuinely-clear title needs no key; the gate passes it.
- `(None, false)` — scrambled but no key recoverable → hard failure via
  `Self::ensure_title_decryptable`, never a silent garbage mux.

## inject_unit_keys

Injects pre-resolved AACS unit keys into a scanned disc — the deferred-mux /
resume path. The keys come from the mapfile's `# freemkv-uk:` header
(persisted at sweep time when the disc was keyed), so the mux decrypts
directly with NO key-service round-trip. Populates `self.aacs.unit_keys` so
`Self::decrypt_keys` returns them and marks the source `ExternalUk`.

If the scan built no AACS state (`self.aacs == None`) — which happens when
the keydb was absent at scan time (`scan_aacs_no_keydb` ->
`aacs_error = KeydbLoad`) — this synthesizes a minimal `ExternalUk` state
for an encrypted AACS disc. A Unit Key is the FINAL per-title decryption
key; the keydb is only needed to *derive* it, and that derivation already
happened at sweep (the UK is in the mapfile). So a UK alone is sufficient to
decrypt the on-disk ISO — AACS 2.0 bus decryption was applied by the drive
at read time, so `read_data_key` is unused for file-backed mux. Without
this, a keyed disc swept without a keydb would recover its UK yet still
report E8005 (no usable `decrypt_keys`) at remux. No-op for an unencrypted
or CSS (DVD) disc.

## scan_with_passes_the_halt_flag_to_the_bluray_enumerator

Every BD and DVD cancellation test in this crate calls
`scan_bluray_titles` / `scan_dvd_titles` DIRECTLY, so the three-line wiring
in `scan_with` that connects the operator's Stop to them was covered by
nothing at all: replacing `opts.halt.as_ref()` with `None` on either branch
left the whole suite green while a Stop during a BD or DVD scan silently did
nothing. Only the HD-DVD branch was pinned.

The fixture is deliberately the smallest disc that takes the BD branch — a
bare `/BDMV` with no PLAYLIST — because the point under test is the
ARGUMENT, not the enumerator's own (separately tested) halt polling. Nothing
between `scan_with`'s entry and the enumerator reads the flag (the disc is
unencrypted, so the AACS path is skipped), so a green here can only mean the
flag arrived. Mutation: `Self::scan_bluray_titles(reader, &udf_fs, None)?`
fails here.

## halted_reads_do_not_report_the_hddvd_scan_as_successful

A Stop on a LIVE DRIVE never touches `ScanOptions::halt`: `Drive` has its
own flag and `checked_exec` fails every SCSI command with `Error::Halted`
once it is set. The HD-DVD enumerator must not swallow that into a
successful scan. Measured before this was fixed: the scan returned `Ok`
with both titles present and ZERO streams on each — a cancelled scan
wearing the shape of a disc whose clips carry no video or audio. Downstream
that is a title list to cache, display and rip from.

## scan_with_capacity_bytes tests

`scan_with`'s `capacity_bytes = capacity as u64 * 2048` feeds
`canonical_title_order`'s "bigger than the whole disc = play-all composite"
threshold.

`..._not_addition`: chosen so `capacity * 2048` clears both titles (neither
is oversize, so the bigger one — OTHER — ranks first), but
`capacity + 2048` lands BETWEEN the two sizes: MAIN stays non-oversize while
OTHER flips to oversize and gets demoted, changing `titles[0]`. A `*` -> `+`
mutation is caught by this ranking flip, not merely a wrong numeric
threshold value.

`..._not_division`: same mechanism, tuned to catch a `*` -> `/` mutation
instead: `capacity * 2048` clears both titles, but `capacity / 2048`
collapses to a threshold far below MAIN's declared size while still leaving
MAIN under it, so only OTHER flips to oversize.

## read_mkb_content_grows_prefix_past_16mib_when_records_run_longer

This fixture's record stream is deliberately built so the first 16 records
land EXACTLY on the 16 MiB boundary (so the 16 MiB prefix alone parses as a
clean, complete record stream — the case a naive "n < buf.len() means done"
check would wrongly accept) and 4 more MiB of records follow, terminated by
the explicit `00 000000` end marker. Only a caller that actually grows past
16 MiB recovers the true 20 MiB content length.

## main_feature_order_demotes_wrapper_composite_decoy

The decoy `00245.mpls` is a WRAPPER COMPOSITE — `[00339 2s bumper][00001 the
whole feature clip][00336 outro]` — so its first-PlayItem STN reports v=0,
and it is the LONGEST and LARGEST title (8350s / 78.4 GB vs the feature's
8038s / 76.5 GB). Muxing its zero frames produced E6008 forever. It must be
demoted STRUCTURALLY (its clip set properly contains the feature's), not by
the fragile first-PlayItem stream count — this is what makes the fix robust
to the video-bearing-composite variant (see
`main_feature_order_demotes_video_composite`).

## key_source_failure_is_not_reported_as_a_missing_disc_key

Drives the real chain end to end — `KeySource` -> `resolve_and_apply_traced`
-> `Disc::aacs_error` -> `ensure_decryptable` — twice over the SAME disc,
and asserts the two operator-visible verdicts differ:

- source returns `Err(KeyServiceUnavailable)` (the HTTP-502 outage) → E7028
  "the key service could not answer; retry later",
- source returns `Ok(vec![])` (the service answered, no entry) → E7022 "no
  key source has a decryption key for this disc".

The credential and rate-limit verdicts are asserted alongside, since each is
a different operator action (fix the token / back off).

## css_disc_wide_no_key_is_disc_level_while_per_title_stays_skippable

The two CSS no-key conditions are NOT the same verdict and must classify
oppositely through the public predicates:

- **disc-wide** — `css_error` is set: the MAIN feature's crack failed, so
  every title of this disc fails identically. Must be
  `crate::error::is_disc_level_no_key` (the rip loop fail-fasts) and must
  NOT be `crate::error::is_skippable_title_stub`. While both conditions
  shared `E_CSS_KEY_MISSING`, an uncrackable CSS disc iterated all N titles
  logging "title skipped" and exited 0 — a total failure reported as
  success.
- **per-title** — one title's own re-crack failed on a multi-VTS disc
  (`title_is_clear == false`, no key): skipping it and finishing the rest is
  correct policy, so it must STAY skippable and must NOT be disc-level.

Pinned in both directions so a future change cannot silently flip either.

## correct_truehd_channels_full_correction_and_atmos_promotion

Kills the `was_basic ==`, the channels/rate `!=`/`&&` guards' "already
correct" branch, the `!matches!` per-stream skip, the `is_atmos ==
Some(true)` branch, and the whole-function no-op mutant.

## byte_offset_in_title_accumulates_across_extents

Kills: `lba >= start && lba < end` flipped to `||` (the first extent's
disjunction would trivially match almost any lba and return the wrong,
too-early offset); `cumulative +=` flipped to `*=` (cumulative is seeded at
0, so `*=` freezes it at 0 forever); and `sector_count * SECTOR_BYTES_U64`
flipped to `+` or `/` (wrong per-extent byte length folded into cumulative).

## chapter_at_offset_concrete_arithmetic

Kills: the `/` (time-fraction) flipped to `%` or `*`; the `*` (duration
scale) flipped to `+`; the `<=` chapter-scan comparison flipped to `>`; the
final `chapter_idx + 1` flipped to `-` or `*`; and every whole-function
fixed-tuple replacement (none produce `(2, 60.0)`).

## locate_ranges_positive_bps_duration_and_at_risk

Kills `bps > 0.0` flipped to `==`/`<` (both would take the `else 0.0`
branch here, wrongly reporting 0); `(*size as f64) / bps` flipped to `%` or
`*`; and the trailing `* MILLIS_PER_SEC` flipped to `+` or `/`.

## decrypt_keys_for_title_css_span_reuse_is_half_open

A CSS title key is per-VTS: reusing the scan's cracked key for a title that
lives OUTSIDE the cracked span descrambles that title with the wrong key,
and the mux emits garbage at exit 0. `crack_span` is documented as the
half-open LBA span `[start, end)`, so overlap is `extent.start < span.end &&
span.start < extent.end` — both comparisons STRICT. A title that merely
ABUTS the span (ends exactly where it begins, or begins exactly where it
ends) shares no sector with it and must NOT reuse the key.

The reader serves only clear (all-zero) sectors, so a title that falls
through to its own crack is reported unencrypted — distinguishable from the
reused-key answer both in the key and in the is-clear flag.

## merge_content_key_ranges

Merges per-title AACS key ranges into the sorted, disjoint set the
whole-disc map needs (`crate::decrypt::AacsKeyMap::entry_for` requires
disjoint ranges). Titles that share a clip resolve the SAME physical span
(same LBAs -> same CPS unit -> same key). When a later range overlaps a kept
one that carries the SAME key index and phase, the two are UNIONED (end
extended to the max) — this covers both the exact-duplicate (shared clip)
case and any partial overlap without ever dropping coverage, so no encrypted
LBA is left in no range (which would pass through as ciphertext). A real disc
never produces two DIFFERENT keys for one LBA; if that malformed case ever
appeared, the later range is dropped to keep the set disjoint rather than
extend one key over another key's LBAs.
