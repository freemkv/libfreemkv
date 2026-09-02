# Stream URL resolver

`src/mux/resolve.rs` parses URL strings (`scheme://path`) into PES stream
instances.

## Scheme table

| Scheme | Input | Output | Path |
|--------|-------|--------|------|
| disc:// | Yes | -- | empty (auto-detect) or /dev/sgN |
| disk:// | Yes | -- | alias for `disc://` (identical behavior) |
| iso://  | Yes | -- | file path (required) |
| mkv://  | Yes | Yes | file path (required) |
| m2ts:// | Yes | Yes | file path (required) |
| network:// | Yes (listen) | Yes (connect) | host:port (required) |
| stdio:// | Yes (stdin) | Yes (stdout) | empty |
| null:// | -- | Yes | empty |
| demux:// | -- | Yes | directory path (required) — per-track ES demux |
| fvi://  | -- | Yes | file path (required) — per-picture video index |

Bare paths without a scheme are rejected.

## `image_input` (shared IMAGE-level source body)

`iso://` and `dir://` differ only in how the sectors are produced — a file
versus a synthesized UDF volume over a folder — and not at all in what is
done with them: scan, apply caller-resolved keys, gate on decryptability,
select the title, prune streams, correct TrueHD channel counts, mux. One
body means a `dir://` source cannot drift away from the `iso://` behaviour
that has years of fixes in it.

`reopen` yields a SECOND, independent reader for the TrueHD channel probe,
which must not disturb the mux reader's position. It returns `Option`
because a failed re-open is non-fatal: the correction is skipped, not the
mux.

## `FMTS_POOL_TAG_BASE`

CPS-unit id given to an FMTS forensic **index key** banked into the caller's
key pool by `resolve_fmts_key_map` (`FMTS_POOL_TAG_BASE + slot`).

The pool entry's first field is a CPS-unit number that the mapped decrypt
never reads — it indexes the pool by SLOT (`crate::decrypt::AacsKeyMap`
ranges carry slots) — so the field doubles as the tag that separates the
disc's BASE CPS unit keys from the forensic index keys appended on top of
them. Base ids come from a key's position in `Unit_Key_RO.inf` (+1), whose
count is a BE16, so they cannot exceed 65_536; this base is far above that,
so the two id spaces can never collide and misclassify a base key as
forensic (which would under-count the CPS units and hand the wrong key to a
whole extent).

## `single_base_key_slot`

The pool slot of the disc's ONE base CPS Unit Key, or `None` when the pool
holds several (a genuine multi-CPS disc) — the forensic index keys
(`FMTS_POOL_TAG_BASE`) that `resolve_fmts_key_map` appends to the same pool
are excluded, so the answer is a property of the DISC and not of how many
titles have already resolved through the shared pool.

## `base_slot_for_extent`

Which CPS unit's BASE Unit Key opens `ext`, as a pool slot — decided by this
extent's own ciphertext, exactly like the multi-CPS path in
`resolve_mux_key_map_cached`, and memoised in the same per-disc
`CpsUnitCache` so a clip several playlists share is sampled once.

Only base keys are considered: the forensic index keys share the pool but
belong to segment ranges, which are already mapped by tag before this runs.

`last_idx` is the slot resolved for the PRECEDING extent of this title,
carried into an extent with no sampleable encrypted units (nothing to
mis-decrypt). An extent that does have real ciphertext no held or fetched
key opens is a fail-loud `crate::error::Error::DecryptFailed` rather than a
silently wrong key over the whole extent.

## `resolve_fmts_key_map`

FMTS (AACS 2.1) branch of `resolve_mux_key_map`. Returns `Some(map)` when the
disc carries `IndividualSegment.tbl` AND a key source is configured; `None`
otherwise (not FMTS, or no source — the caller's base-Unit-Key path then
applies, and the forensic units garble and are dropped by the demux).

The forensic segments each carry an **index** tag (1..32) selecting one of 32
**index keys** the base Unit Key cannot open (see `crate::aacs::segment`).
This resolves those keys up front from the configured source — sending, per
index, a batch of same-index units the service maps to that index's key — adds
them to the pool, and builds a per-segment LBA→key map. Applying a segment's
key over its whole range decodes the ~40 units of that index's interleave half
to clean TS and garbles the other ~40 (the alternate half), which the demux
then drops, yielding one coherent stream. The base Unit Key covers everything
outside a segment.

The segment SPNs are offsets into the FORENSIC FEATURE CLIP, so every
clip-byte → LBA mapping here is anchored on that clip's own extents
(`forensic_clip_extents`) — never on `title.extents`, which on a play-all
playlist is the concatenation of several clips and would put the segments in
the wrong one.

Everything expensive here is memoised per DISC, not recomputed per title:
`cache.table` holds the UDF walk + `IndividualSegment.tbl` and `cache.clip`
the forensic clip's extents (both disc-invariant outright), and `cache.keys`
holds the index keys + phases. See `resolve_mux_key_map_cached` for why a hit
is provably the same answer. What remains per title is the pool→slot
mapping, the LBA range arithmetic and the base-key gap fill.

The gap fill covers each non-forensic LBA with the base Unit Key of the CPS
unit it belongs to. On the single-base-key disc that is every FMTS disc seen
so far that costs nothing; a disc with several base CPS Unit Keys resolves
each extent from its own ciphertext through `cps` (`CpsUnitCache`, shared
with the multi-CPS path so a clip is sampled once per disc).

## `probe_fmts_index_keys`

The drive- and key-service-hitting half of `resolve_fmts_key_map`: anchor the
disc's whole forensic index-key set from ONE key-service round trip, then
probe each index's interleave phase. Split out so the result can be memoised
per disc (`FmtsKeyCache`) — the arithmetic that turns these keys into a
per-title LBA map stays at the call site, where the title belongs.

`clip_extents` are the FORENSIC FEATURE CLIP's own extents — the byte space
the segment SPNs are relative to — and `segments` those addressable within
it; every read is `clip_byte_to_lba(clip_extents, …)`, so a probe never lands
in another clip's sectors whatever order a playlist lists its clips in.
Neither probe reads the caller's key pool, which is what makes the result
independent of resolve order across titles.

## `forensic_clip_extents`

The FMTS forensic feature clip's own extents — the byte space every
`IndividualSegment.tbl` SPN is relative to — or `None` when the disc does not
identify exactly one such clip.

An AACS 2.1 disc names its forensic feature `BDMV/STREAM/<clip>.fmts` (the
extension `bluray.rs` already resolves clip extents through), and the disc
carries ONE segment table, whose SPNs are therefore in ONE clip's byte space.
So: exactly one `.fmts` file → that clip's extents; none, or several (an
ambiguous SPN space), → `None`, which `resolve_fmts_key_map` turns into a
loud `Error::FmtsKeyMissing` rather than a guess.

This is a DISC fact — no title is consulted — which is exactly why it is a
sound anchor: a playlist's `extents` are the concatenation of ALL its clips
in playback order, so their byte space is the playlist's, not the forensic
clip's.

A read fault on the clip's ICB propagates (`Err`), like every other read on
this path; a purely structural absence is `Ok(None)`.

## `filter_addressable_segments`

Keep only the forensic segments addressable within the FORENSIC CLIP's
extents: a segment whose clip-byte start (`start_spn * 192`) maps to an LBA
inside the clip is real forensic content; one that does not is past the
clip's end (a stale or foreign table record) and is dropped. An empty result
means there is nothing forensic to resolve, so `resolve_fmts_key_map` returns
`Ok(None)` and the caller's base Unit-Key path applies. Extracted from
`resolve_fmts_key_map` for direct testing of the inclusion/exclusion
decision.

## `resolve_tie_phase`

Decide a forensic index's decrypt phase from the clean-sample counts of its
EVEN vs ODD aligned units under that index's key. Extracted from
`resolve_fmts_key_map` so the tie logic is unit-testable; the `tracing`
diagnostics stay at the call site, which holds the segment-index context.

* `even > odd` → `Phase::Even`; `odd > even` → `Phase::Odd` — the clean half
  is this index's real content variant.
* `even == odd == 0` → `Error::FmtsKeyMissing`: NEITHER half decrypts clean,
  so the key is wrong (or the sample is not this index's content) — fail
  loud rather than emit a broken segment.
* `even == odd > 0` → `Phase::Even`: BOTH halves are clean, i.e. source-zero
  padding (clean under any key), so the parity is immaterial — default Even.

## `IndexProbe`

Outcome of probing ONE forensic index's decrypt phase (see
`probe_index_phase`). The load-bearing distinction is between the last two: a
genuine wrong key and a transient live-drive read fault both leave zero clean
decrypts, but only the former is a real `FmtsKeyMissing` — the latter must
NOT abort a rip whose index keys are valid.

## `probe_index_phase`

Probe one forensic index's decrypt phase by reading a representative
segment's EVEN vs ODD aligned units and counting clean decrypts under `key`.
Extracted from `resolve_fmts_key_map` so the read-fault-vs-wrong-key decision
is directly testable without a full UDF/segment-table fixture.

Mirrors the anchor loop's read-fault tolerance: try up to `max_segments`
same-index segments (rather than a single `.find`), skipping any whose reads
all fault, and only conclude `IndexProbe::WrongKey` once a segment actually
yielded decrypt attempts. If EVERY read of EVERY same-index segment faults,
return `IndexProbe::ReadFault` — the caller then leaves the phase unresolved
(defaults to `Phase::All`) instead of hard-aborting the rip. `read(seg,
unit)` reads aligned unit `unit` of `seg`; `None` is a read fault.

Masking guard: `IndexProbe::ReadFault` is returned ONLY when not a single
read succeeded, so a genuine wrong key (whose reads DO succeed) can never be
masked as a read fault — any successful, non-clean decrypt yields
`IndexProbe::WrongKey`.

## `fill_base_key_gaps`

Back-fill the LBA gaps NOT covered by the forensic segment ranges with the
base Unit Key, so the finished map is a COMPLETE positive list over the
title's content extents: every content LBA resolves to either a forensic key
(inside a segment) or the base key (`base_idx`). An LBA left in no range
would pass ciphertext through as clear — this range arithmetic guarantees
there is no such hole inside any extent. Extracted from `resolve_fmts_key_map`
for exhaustive direct testing (gaplessness over every extent).

`forensic_ranges` are the already-built per-segment ranges; only their
`[start, end)` spans matter here (they carve the holes — the key idx / phase
are irrelevant). The return is the base-key fill ranges ONLY; the caller
appends them to `forensic_ranges` to form the full map.

## `resolve_mux_key_map`

This is what ends the key-server storm. The old mux decrypted a unit, checked
whether the plaintext looked like clean MPEG-TS, and — because authored-bad
content never reaches that bar — re-asked the key service for a key it
already held. There is no per-unit byte pattern that separates "correctly
decrypted but authored-bad" from "still encrypted", so that check is
unanswerable. Here we answer the answerable question instead: which CPS unit
does each LBA range belong to, decided by the disc's key structure (validated
once against real ciphertext samples, where the `is_clean` proof IS sound).
The mux then just decrypts each unit with its mapped key and trusts it.

Single-CPS (the overwhelming majority, incl. every single-key UHD) keys every
content extent with one index; multi-CPS keys each extent with the key that
opens a real sample from it; FMTS layers per-segment index keys on top. Any
LBA outside the title's content (nav/filesystem) is in no range and passes
through.

## `CpsUnitCache`

Memoises the multi-CPS "which held unit key opens this extent" decision
across the titles of ONE disc, for `resolve_mux_key_map_cached`.

Keyed by content format plus the extent's exact `(start_lba, sector_count)`,
so a hit returns the index that was resolved from *those same physical
bytes* — see the safety argument on `resolve_mux_key_map_cached`. Only
successfully resolved extents are memoised; a no-samples extent (whose index
is inherited from the preceding extent of the SAME title, i.e. not a
property of the extent) and the fail-loud "no key opens it" outcome are
never cached.

A disc's playlists overwhelmingly reference the same handful of clips (main
feature, play-all, per-chapter and seamless-branch variants), so without this
the same extents are re-sampled off the drive once per playlist: 8 random
6144-byte reads each, ~200 ms of seek apiece on a stock BD drive.

Scope, stated precisely: this memo covers the extent-sampling reads that
decide which CPS unit an extent belongs to, and nothing else. On an FMTS
(AACS 2.1) disc `resolve_fmts_key_map` runs first and returns a finished map
before the extent loop below is ever reached, so the loop's reads are
removed by `FmtsTableCache` and `FmtsKeyCache` instead — but a MULTI-CPS
FMTS disc samples through this same memo from the gap fill
(`base_slot_for_extent`), and the cached value means the same thing on both
paths: the pool slot whose key opens that extent's own ciphertext.

## `FmtsTableCache`

The disc's forensic segment table (`/AACS/IndividualSegment.tbl`), resolved
at most ONCE per disc: `None` = not looked for yet; `Some(None)` = looked for
and this disc is not FMTS; `Some(Some(v))` = the parsed, non-empty segment
list.

Every input is a property of the MEDIA, not of a title: the UDF walk
(`crate::udf::read_filesystem`) reads fixed low LBAs (the anchor at 256, the
VDS, the FSD, the root directory), `read_file` follows that file's own
allocation descriptors, and `parse_individual_segments` is pure. Re-running
it per title re-reads ~35 single sectors at low LBAs from a head the
previous title's content sampling left deep in the content area — a
full-stroke seek out and back per playlist, for a byte-identical answer.

Only the two *deterministic* negatives are memoised as "not FMTS"
(`UdfNotFilesystem`, and `UdfNotFound` for the table): a read fault is NEVER
cached, so a transient failure still propagates and a later title still
retries.

## `FmtsKeyCache`

Memoises the FMTS (AACS 2.1) forensic **index-key set and per-index phase** —
the expensive half of `resolve_fmts_key_map` — across the titles of ONE disc.

Keyed by `(format, the title's exact extent list)`, mirroring
`crate::disc::pgs_forced_probe::ForcedProbeCache`. Every read the anchor
probe, the phase probe and the key-service call make is
`clip_byte_to_lba(forensic_clip_extents, …)` and the probed segments are
`filter_addressable_segments(_, forensic_clip_extents)` — both DISC facts
(see `forensic_clip_extents`), so the answer no longer varies by title at
all. The extent list is KEPT in the key deliberately: it is finer than the
answer needs, so each distinct extent list still gets its own verdict rather
than inheriting another's, and a title can never be served an answer
resolved from bytes it does not read. Two titles with the same extent list
feed byte-identical samples to a stateless `crate::sector::KeyFetch` and to
the same `is_clean` arithmetic under the same `format` — see the safety
argument on `resolve_mux_key_map_cached`.

The value holds the ordered index keys (element `i` = forensic index `i + 1`)
and the resolved phase per index tag. It is key material: never logged,
never rendered, and dropped with the disc's resolve.

## `FmtsClipCache`

The disc's forensic feature clip extents, resolved in the SAME UDF walk as
`FmtsTableCache` and at most once per disc: `None` = not looked for yet;
`Some(None)` = looked for and not identifiable (or the disc is not FMTS);
`Some(Some(v))` = the clip's extents, the anchor for every segment SPN.

A disc fact like the table itself — see `forensic_clip_extents` for why, and
for what `Some(None)` costs on an FMTS disc (a loud `FmtsKeyMissing`, not a
guess).

## `resolve_mux_key_map_cached`

`resolve_mux_key_map` with a caller-owned per-disc cache (`DiscKeyCache`)
shared across the titles of one disc.

### Why a cache cannot change a resolved key

#### Multi-CPS extents (`CpsUnitCache`)

The cached value is the pool index `pick` chose for an extent, and every
input to that choice is stable across the titles of one disc:

* The samples are a deterministic function of the extent's `(start_lba,
  sector_count)` and `format` — both in the key — read from unchanging media.
* The key pool is APPEND-only here (base-key fetch and the FMTS resolver only
  push), and `pick` returns the FIRST pool entry that opens a sample, so a
  later, longer pool yields the same first match for the same samples.
* `crate::sector::KeyFetch` is stateless by contract, and any key a hit's
  index refers to was already banked into the pool on the miss that filled
  the entry — so skipping the re-fetch skips no side effect the map depends
  on.

#### FMTS segment table (`FmtsTableCache`)

Disc-invariant outright: no input to the UDF walk, the `read_file` or the
parse mentions the title. See the type's doc for the negatives that are
(and are not) memoised.

#### FMTS forensic clip extents (`FmtsClipCache`)

Disc-invariant outright, like the table: the clip is found by name in the
UDF tree and its extents come from its own allocation descriptors. No title
is consulted — which is the point. It is the byte-space anchor for every
segment SPN, and anchoring on a TITLE's extent list instead made a playlist
that lists a trailer before the feature map every segment into the
trailer's sectors.

#### FMTS index keys and phases (`FmtsKeyCache`)

Every input to the anchor and phase probes is now a disc fact: the probes
read `clip_byte_to_lba(forensic_clip_extents, …)` and visit
`filter_addressable_segments(_, forensic_clip_extents)`. The title's extent
list stays IN the key anyway — a strictly finer key than the answer needs,
so no title inherits a verdict resolved from bytes it does not read. Given
the same key, the same `format` and unchanging media:

* `filter_addressable_segments` yields the same segments in the same order,
  so the anchor loop and each `probe_index_phase` visit the same segments.
* every probe read is at the same LBA, so the same ciphertext is fed to a
  `crate::sector::KeyFetch` that is stateless by contract, and to the same
  `aacs_unit_encrypted` / `decrypt_unit` / `is_clean` arithmetic.
* NEITHER probe reads the key pool. The anchor takes its keys from `fetch`;
  the phase probe takes them from the anchor's reply. So the pool's growth
  across titles — the one thing that does change between calls — cannot
  move this result, and the cached value is independent of the order titles
  are resolved in.

What is deliberately NOT memoised, which is what makes this safe rather than
merely faster:

* the fail-loud `FmtsKeyMissing` verdicts (no anchor, wrong key), so a retry
  after a key source is reconfigured re-probes rather than inheriting
  nothing;
* a run where any index's phase probe came back `IndexProbe::ReadFault` —
  that leaves the phase defaulted to `Phase::All` (degraded but complete), a
  property of a transient live-drive fault and NOT of the extents. Caching
  it would spread one bad read across every remaining title.

Everything downstream of the cache still runs per title: the pool insertion
that turns index keys into slots, the per-segment LBA range arithmetic, and
the base-key gap fill — all of which genuinely depend on this title's
extents.

Halt is polled per extent AND once on entry to the FMTS branch, so a
60-playlist sweep stays cancellable even when every title is served from
cache.

## `build_iso_pipeline` parameters

Assembles the ISO mux pipeline (read+decrypt → demux → parse) for a
`FileSectorSource`-backed reader. Returns the resulting `PipelinedPesStream`.

- `reader`: the sector source to read from (typically a `FileSectorSource`
  over the ISO image).
- `title`: the selected title; its `extents` drive the read range and its
  `streams` build the demux/parse tables.
- `keys`: decryption keys applied per sector batch. Pass
  `crate::decrypt::DecryptKeys::None` for raw / unencrypted reads (the
  decrypt decorator then becomes a pass-through).
- `batch_sectors`: read batch size in logical (2048-byte) sectors — a
  throughput/latency tuning knob, not a correctness parameter.
- `format`: container format (`BdTs` → TS demuxer, `MpegPs` → PS demuxer).
- `raw`: ciphertext passthrough. When `true`, the per-title CSS crack
  (`resolve_dvd_title_key`) is skipped entirely — no key is resolved and a
  scrambled title is neither descrambled nor hard-failed.
- `halt`: cooperative cancel token (not a timeout); when cancelled the
  pipeline stops at the next boundary (and the CSS crack surfaces `Halted`).
  `None` disables cancellation.
- `event_fn`: optional progress/event callback invoked by the prefetcher.
- `fetch`: optional key source used UP FRONT by `resolve_mux_key_map` to
  secure any CPS-unit key the pool is missing. Not a per-unit mux-time
  callback: the map decides the key for every LBA before the read loop
  starts.

Nine reader/title/keys/tuning/callback params is inherent to the mux entry
point; grouping them into a struct would only move the same fields around.

## `parse_url_never_panics_on_adversarial_input`

`parse_url` must never panic on ANY input — it is the front door for
caller-supplied URL strings, so a panic here would crash the binary on
malformed input instead of surfacing a clean error downstream. Feed it a
battery of adversarial strings (empty, doubled/garbled schemes, embedded
NUL, unicode, a very long path, lone scheme markers) plus an exhaustive
sweep of every single byte 0x00..=0xFF as the whole input and as a scheme
suffix. Any `StreamUrl` variant is an acceptable result; the only failure
mode under test is a panic.

## `fvi_output_records_the_source_not_the_destination`

`fvi://` must record the SOURCE in `source.{path,medium,title}`. It used to
pass the DESTINATION path as `FviSink::create`'s `source_path` (and default
the medium/title), so every index claimed to be its own source —
`docs/FVI_FORMAT.md` §6.2 defines `source` as describing the input.

## `fvi_output_is_reproducible_across_destination_paths`

The property the destination-as-source defect broke: two runs indexing the
SAME source must produce byte-identical output regardless of where they
write. Previously the header embedded the destination path, so the two
files differed (and differed in length when the paths differed in length)
purely from where they landed.

## `parse_dir_url_is_an_image_source_unlike_the_directory_sinks`

`dir://PATH/` parses to `StreamUrl::Dir` with the raw remainder as the path.
Unlike the other directory schemes it IS an image-level source (1.6.1):
`crate::dirimage` synthesizes a UDF volume over the folder, so
`is_disc_source()` — "has a filesystem to scan" — is true for it and false
for the write-only `demux://` / `fvi://` directory sinks.

## `dir_url_is_an_input_but_never_a_pes_sink`

`dir://` is never a PES SINK — it writes raw decrypted files, not muxed
frames, so `output()` still rejects it (StreamReadOnly → Unsupported) and
the CLI routes a `dir://` dest to `Disc::extract_tree`.

As a SOURCE it is no longer rejected out of hand: it is an image source, and
a missing folder now fails as a missing folder (NotFound) rather than as
"this scheme cannot be read".

## `build_demux_state_bdts_builds_ts_demuxer_and_pid_table`

BdTs format must build a TsDemuxer (Some(ts), None(ps)) when there is at
least one PID, and one parser + pid_to_track entry per stream keyed by the
stream's own PID. (Mis-keying here is exactly the class of bug that
mis-routes PES into the wrong codec parser.)

## `HaltCountSource`

A counting `SectorSource` over zeros. `touched_extent` flags whether any
read landed in the title's extent region (LBA >= 1000); the UDF probe only
reads near LBA 256 (small `capacity`), so a hit there means the expensive
per-extent `sample_units` loop ran.

## `resolve_mux_key_map_honors_pre_cancelled_halt`

`resolve_mux_key_map` on the multi-CPS live path must honor a pre-cancelled
halt PROMPTLY — `Err(Halted)` at the first extent boundary, before sampling
any extent's ciphertext — rather than reading through every extent. This is
the round-2 Fix 1 guard: the resolve chain runs on the LIVE drive (each
`read_sectors` can stall to the SCSI recovery timeout), so an operator Stop
during key resolution must interrupt it.

Mutation: dropping the `halt.is_some_and(...) → Err(Halted)` check in the
multi-CPS extent loop makes the resolve run the sampling reads and return
`Ok(map)` (zeros sample to no encrypted units → carry key 0), so
`expect_err` fails AND `touched_extent` flips true.

## `bdts_data_packet`

Build a 192-byte BD-TS data packet on `pid` carrying `payload` as the TS
payload (payload-only adaptation). Layout: 4-byte TP_extra_header (zeros) +
188-byte TS packet (sync 0x47, PID, PUSI, AFC=0b01). Mirrors the BD-TS
framing in ts.rs.

## `build_iso_pipeline_empty_extents_clean_eof`

Empty extents → the producer thread exits immediately, the demux thread sees
a clean channel close and emits the Eof sentinel, and the PipelinedPesStream
returns Ok(None) on the first read. The highway must terminate cleanly (no
panic, no hang) when there is nothing to read.

## `build_iso_pipeline_delivers_one_frame_then_eof`

End-to-end: one BD-TS packet carrying a complete audio PES flows read →
decrypt(passthrough) → TS demux → codec parse → one PesFrame. Proves the
full highway wiring delivers the ES payload intact and reaches a clean EOF
afterward (never silently truncating the frame).

## `build_iso_pipeline_pruned_title_drops_unselected_pid_frames`

End-to-end proof of stream selection: a title declaring TWO audio PIDs,
pruned to one via `StreamSelection::apply` BEFORE `build_iso_pipeline`, must
never surface a frame from the excluded PID. The demuxer is built from the
pruned `title.streams`, so the excluded PID is untracked and its packets are
skipped — track headers and frames both follow the pruned list, which is
the whole point of the selection seam.

## `build_iso_pipeline_dvd_none_keys_scrambled_hard_fails`

REGRESSION (autorip production corruption): `build_iso_pipeline` for a DVD
(MPEG-PS) with `None` keys — what autorip's mux passes on a detection-miss
DVD (`disc.decrypt_keys()` == None) — must resolve the CSS key from the
reader itself. A scrambled-but-uncrackable title must HARD-FAIL, never build
a passthrough pipeline that muxes the scrambled sectors as corrupt video.
Before this fix, autorip handed None straight through and the mux wrote
garbage at exit 0.

## `content_map_builds_exact_ranges_from_extents`

`content_map(title, idx)` keys every single-CPS UHD disc (the common case):
each content extent → one `[start_lba, start_lba+sector_count)` range at
`idx`, phase `All`. Assert the exact ranges — an off-by-one on the end (or a
wrong idx / phase) must flip this test to FAIL.

## `filter_addressable_segments_keeps_only_in_title_segments`

BEHAVIOR 1 — segment filter (`resolve_fmts_key_map` line ~800). A segment
whose clip-byte start (`start_spn * 192`) maps inside the title's extents is
kept; one whose start is past the clip is dropped; all-outside → empty (the
resolver then returns `Ok(None)` and the base-UK path applies).

## `resolve_tie_phase_covers_all_arms`

BEHAVIOR 2 — phase-tie default (`resolve_fmts_key_map` line ~936). All four
arms of the even/odd clean-count decision.

## `assert_gapless`

Assert `forensic` + `fills` together cover every LBA of every extent EXACTLY
once — no gap (a hole would pass ciphertext through as clear) and no
overlap (two keys over one LBA). This is the load-bearing invariant of the
gap-fill.

## `fill_base_key_gaps_is_gapless_over_every_extent`

BEHAVIOR 3 — gap-fill range arithmetic (`resolve_fmts_key_map` line ~1005).
Exhaustive: no segments, mid-extent, at-start, at-end, adjacent segments,
and multi-extent. Each asserts the EXACT fills AND gaplessness over every
extent — an off-by-one that leaves a hole flips this to FAIL.

## `encrypted_clean_unit`

Build a 6144-byte aligned unit of CLEAN MPEG-TS (sync `0x47` + non-zero
payload in packets 1.., packet 0 is the clear seed) then AACS-encrypt it
under `key`. Decrypting under the SAME key restores clean TS (`is_clean` →
true); decrypting under any other key yields garbage.

## `probe_index_phase_all_faults_is_read_fault_not_wrong_key`

A probe whose EVERY read faults (`read` returns `None`) must classify as
`IndexProbe::ReadFault`, NOT `IndexProbe::WrongKey` — a transient live-drive
read fault while probing must not be read as a missing key (which the
caller turns into a rip-aborting `FmtsKeyMissing`).

Mutation: reverting to the no-fallback single-segment probe (i.e. treating
even==odd==0 as unconditional `FmtsKeyMissing` regardless of whether any
read succeeded) makes this return `WrongKey` → the assert fails.

## `probe_index_phase_reads_succeed_but_no_clean_phase_is_wrong_key`

Reads SUCCEED but decrypt to NEITHER clean parity (ciphertext under a key we
do NOT hold) → `IndexProbe::WrongKey`. This is the genuine-missing-key path
the caller MUST keep as a hard `FmtsKeyMissing`.

## `probe_index_phase_falls_through_faulting_segment_to_next`

Read-fault TOLERANCE across segments: the first same-index segment faults on
every read, but a SECOND same-index segment decrypts clean → the probe must
fall through to it and resolve a phase (mirrors the anchor loop's
multi-segment retry). A single-`.find` probe would have stopped at the
faulting first segment.

## `CipherSource`

A SectorSource that tiles a fixed 6144-byte ciphertext unit across each
registered extent range (`(start_lba, end_lba, unit)`), zeros elsewhere.
Every 3-sector aligned-unit read inside a range returns the same ciphertext,
so `sample_units` collects real encrypted content for `pick`/fetch to run
on. Low LBAs are zero, so `udf::read_filesystem` fails → the FMTS branch
returns `Ok(None)` and the multi-CPS path is exercised.

## `resolve_mux_key_map_multi_cps_pick_selects_correct_index`

`pick()` must select the pool index of the key that actually opens the
extent's real ciphertext — index 2 here, NOT 0. Feeds units encrypted under
the third pool key through the live multi-CPS path.

Mutation: `pick` hard-returning `Some(0)` keys the extent to 0 → this assert
(Some(2)) fails.

## `resolve_mux_key_map_multi_cps_fail_loud_on_absent_key`

Fail-loud: a sample that decrypts clean under NO held key and NO fetched key
(fetch = None) must surface `Error::DecryptFailed`, never silently key the
extent to a neighbour's (wrong) index.

Mutation: dropping the `None => Err(DecryptFailed)` guard (e.g. falling back
to `last_idx`) returns `Ok` → this `expect_err` fails.

## `resolve_mux_key_map_multi_cps_fetch_recovers_missing_key`

KeyFetch cold path: the pool is missing the extent's key, but the injected
`KeyFetch::unit_keys` returns it from the failing samples → the extent
resolves to the newly-appended pool index (2) and the map succeeds. Proves
the on-miss fetch+re-pick branch runs end to end.

## `CountingCipherSource`

`CipherSource` plus a counter of CONTENT reads (`lba >= CONTENT_LBA_FLOOR`) —
the `sample_units` probes whose cost this cache exists to remove. Low-LBA
UDF metadata probes are excluded so the counts speak only about extent
sampling.

## `multi_cps_shared_extent_is_served_from_cache_not_resampled`

A disc's playlists overwhelmingly share clips, and the multi-CPS path issues
8 random 6144-byte reads per extent. The SECOND title over the same extent
must cost ZERO further reads and resolve the SAME index; a DIFFERENT extent
must still be sampled and get its own index.

Mutation: dropping the `cache.get` short-circuit re-samples → the
zero-further-reads assert fails. Caching the wrong index (e.g. inserting
`last_idx`) breaks the same-index asserts.

## `multi_cps_cache_hit_matches_a_full_recompute`

The cached index must be exactly what a full recompute produces: resolve the
same title twice, once through a warm shared cache and once through a cold
one (a real re-read), and compare the maps range for range.

Mutation: caching under a key that ignores `start_lba`/`sector_count`, or
storing anything but `pick`'s index, diverges here.

## `multi_cps_inherited_index_is_not_cached`

An extent with no sampleable encrypted units inherits the PRECEDING extent's
index — per-title state, not a property of the extent — so it must never be
memoised. Title A reaches the clear extent carrying index 2, title B carries
index 1: B's clear extent must key to 1, not to A's cached 2.

Mutation: caching the `samples.is_empty() => last_idx` arm makes B's clear
extent resolve to 2 and this fails.

## `multi_cps_failed_extent_is_not_cached`

A fail-loud extent (real ciphertext no key opens) must not be memoised: a
later retry, after a key source banked the missing key, has to re-sample and
succeed rather than inherit the earlier failure.

Mutation: caching before the `None => Err(DecryptFailed)` arm (or caching
the error) leaves the retry unable to resolve.

## `FaultSource`

A SectorSource whose every read is a transient I/O fault (`DiscRead`),
modelling a marginal live drive stalling while `resolve_fmts_key_map` probes
the UDF metadata / segment table.

## `resolve_fmts_key_map_read_fault_propagates`

A transient `DiscRead` fault while reading the UDF metadata for the segment
table must PROPAGATE (fail loud / retryable), NOT be swallowed into the
not-FMTS `Ok(None)` fall-through — otherwise a marginal AACS 2.1 disc would
silently drop its forensic content under a base-Unit-Key-only map and the
mux would report success.

Mutation: revert the read_filesystem arm to `let Ok(udf) = ... else { return
Ok(None) }` → this returns `Ok(None)` and the assert fails.

## `resolve_fmts_key_map_not_udf_is_clean_none`

A reader whose bytes are structurally NOT a UDF disc (all zeros → no AVDP at
sector 256 → `UdfNotFilesystem`) is genuinely not FMTS: it must map to the
clean `Ok(None)` negative, NOT fail loud. Guards against Fix 1 over-reaching
and rejecting benign non-FMTS discs.

## `fmts_disc_with_extra_records`

The same synthetic disc with extra records appended to
`IndividualSegment.tbl` ONLY — the content region is laid out from
`fmts_segments` exactly as before, so the anchor and phase probes still
resolve normally and the extra records are seen purely by the range
builder. That is what isolates each of its "this record does not map" arms.

## `FmtsDisc::rebuild_meta` / `with_forensic_clip`

`clip = false` drops `BDMV/STREAM/00001.fmts` from the tree: an FMTS disc
(the table is there) whose forensic clip cannot be identified, so the
segment SPNs have no defensible anchor. `rebuild_meta` returns just the UDF
metadata image, with `tbl_segs` as the segment table and the forensic clip
present — see `fmts_disc_with_extra_records`.

## `FmtsDisc::with_second_cps_unit`

The same disc, plus a second CPS unit occupying the extent at
`FMTS_CPS2_LBA` — a disc whose `Unit_Key_RO.inf` carries two base Unit Keys,
which is what makes "the base key" ambiguous.

## `FmtsDisc::unit_at`

The 6144-byte ciphertext of the aligned unit starting at clip byte
`unit_byte`: inside a segment, EVEN units carry that index's content and ODD
units the alternate variant; outside, ordinary base-Unit-Key content.

## `counting_fmts_fetch`

A `KeyFetch` whose `fmts_indexes` counts its calls and behaves like the real
service: it replies with the disc's COMPLETE ordered index-key set only for
a genuine index-1 anchor batch (one that opens under index key 1), and empty
otherwise. `unit_keys` is never used on this path.

## `fmts_two_cps_title`

A play-all title over BOTH CPS units: the forensic clip (CPS unit 1) then
the second unit's extent (CPS unit 2).

## `fmts_two_cps_keys`

The disc's two BASE CPS Unit Keys, in `Unit_Key_RO.inf` order — the pool an
AACS 2.1 disc with two CPS units is resolved with.

## `fmts_gap_fill_uses_each_lbas_own_cps_unit_key_not_pool_slot_zero`

On an FMTS (AACS 2.1) disc the non-forensic gap fill used to hardcode pool
slot 0 as "the" base Unit Key. On a disc carrying MORE THAN ONE base CPS
Unit Key in `Unit_Key_RO.inf` that is simply the first CPS unit's key, so
every content LBA outside a forensic segment in any OTHER CPS unit was
keyed with the wrong key.

It does not fail loudly: the mapped decrypt runs the wrong key over those
units and emits garbage plaintext with `lost_bytes == 0`. And
`resolve_mux_key_map_cached` calls the FMTS resolver BEFORE the
`single_base_key_slot` short-circuit, so the guard that makes slot 0 correct
on a single-CPS disc is never consulted on this path.

The title spans both CPS units; the second extent's every unit is encrypted
under the SECOND base key (pool slot 1). Mutation: pinning the gap fill back
to slot 0 fails the second extent's asserts, and pinning it to slot 1 fails
the first extent's.

## `fmts_index_keys_resolved_once_per_disc_not_once_per_title`

The defect this fixes: `resolve_content_key_map` resolves EVERY title, and
the FMTS branch used to re-walk the UDF filesystem, re-run the anchor probe,
re-run the 2-index phase probe AND re-ask the key service — once per
playlist — for an answer that is a property of the DISC. N titles must cost
ONE UDF walk, ONE index probe and ONE `fmts_indexes` call.

Mutations, and which assert kills each:
* `if table.is_none()` → `if true` (never memoise the segment table): the
  `meta_reads` assert fails (the walk repeats per title).
* removing the `memo.get(&ek)` short-circuit: the `probe_reads` AND the
  `fmts_indexes` call-count asserts both fail.
* `if probed.all_phases_definite` → `if false` (never insert): same two.

## `fmts_memoised_map_equals_per_title_recomputation`

Result-identity, proven rather than assumed: the same three titles resolved
with a SHARED per-disc memo must produce the same maps — and leave the key
pool in the same state — as resolving each with a FRESH memo (i.e. the
unmemoised per-title recomputation).

Mutation: key the FMTS memo on something NOT title-invariant (e.g. drop the
extent list from `extent_key` and key on `format` alone) → title 3's
differing extents are served the wrong answer and the range comparison
fails.

## `fmts_different_extent_list_is_a_miss_and_reprobes`

A DIFFERENT extent list is a genuine miss: the extent list is the only
per-title input to the probes, so it is IN the memo key and a title with a
different one must re-probe rather than inherit.

Mutation: drop `title.extents` from `extent_key` → the second title hits
and the `probe_reads` / call-count asserts fail.

## `fmts_read_faulted_phase_is_not_memoised`

A phase probe that READ-FAULTED on every segment of an index leaves that
index defaulted to `Phase::All` — degraded but complete. That is a property
of a transient live-drive fault, NOT of the title's extents, so it must NOT
be memoised: caching it would spread one bad read across every remaining
playlist. The next title must re-probe.

Mutation: memoise unconditionally (drop the `all_phases_definite` guard) →
the second title is served from cache and both the `probe_reads` and the
key-service call-count asserts fail.

## `non_fmts_disc_walks_the_filesystem_once_for_every_title`

The UDF walk that decides whether a disc is FMTS at all runs on EVERY disc,
FMTS or not. On a plain (non-UDF / non-FMTS) BD it must be attempted ONCE
for the whole disc, not once per playlist — ~35 low-LBA single-sector reads
reached by a full-stroke seek back from wherever the last title's content
sampling left the head.

Mutation: `if table.is_none()` → `if true` → the second title re-reads the
metadata and the `meta` assert fails.

## `fmts_memoised_title_still_honors_halt`

Halt must stay responsive even when both FMTS memos are warm and a title
does no I/O at all: `resolve_content_key_map` polls nothing itself, so the
entry check inside the FMTS branch is the only cancellation point a
fully-memoised title reaches.

Mutation: remove the `check_halt()?` at the top of `resolve_fmts_key_map` →
the cancelled second title returns `Ok(map)` and `expect_err` fails.

## `single_cps_short_circuit_survives_a_forensic_title_resolving_first`

The single-CPS short-circuit must depend on the number of BASE CPS Unit
Keys, NOT on the length of the whole pool — which the FMTS resolver APPENDS
its forensic index keys to. Before the fix, a disc whose first-resolved
playlist was forensic left the shared pool at `1 + n_index`, so every LATER
title (single-CPS, non-forensic) missed the short-circuit and took the
multi-CPS sampling path: 8 random 6144-byte reads per extent, and a
`DecryptFailed` abort of the WHOLE-disc key map for any extent no pooled
key opened. That made the result depend on the ORDER titles resolve in,
contradicting the order-independence `resolve_mux_key_map_cached`
documents.

Mutation: count the whole pool (`unit_keys.len() == 1`) → the menu title
samples its extent and the `probe_reads` assert fails.

## `forensic_segments_anchor_to_the_forensic_clip_not_the_titles_first_extent`

Forensic segment SPNs live in the FORENSIC FEATURE CLIP's byte space, so
mapping them through the TITLE's extent list is wrong for any playlist whose
extents do not begin with that clip. On a play-all playlist ordered
[trailer, forensic feature] the old code treated clip byte 0 as the
trailer's first sector: every segment landed 300 sectors early, the anchor
probe sampled the TRAILER (so the key service never anchored →
`FmtsKeyMissing` aborted the whole disc), and had it anchored, the index
keys would have been applied to non-forensic sectors — silent garble with
no error at all.

Mutation: anchor the byte space on `title.extents` again → the resolve
either errors (no anchor) or maps segment 1 to 10_000 instead of 10_300.

## `unidentifiable_forensic_clip_fails_loud_rather_than_guessing_an_anchor`

The anchor is only sound because the forensic clip is IDENTIFIED. A disc
that carries a non-empty `IndividualSegment.tbl` but no `BDMV/STREAM/*.fmts`
gives the SPNs no defensible byte space, so the resolve must fail LOUD
(`FmtsKeyMissing`, retryable) rather than fall back to a title's extent list
and map forensic index keys onto whatever clip happens to be first — which
produces silently garbled output with no error at all.

## `single_base_key_slot_counts_cps_units_not_banked_forensic_keys`

The base-CPS count that drives the single-CPS short-circuit, in isolation:
the forensic index keys the FMTS resolver appends (tagged
`FMTS_POOL_TAG_BASE + n`) are NOT CPS units, and a genuine second CPS unit
is.

Mutation: drop the tag filter → the first case returns `None` and its
assert fails.

## `fill_base_key_gaps_sorts_cuts_that_arrive_in_table_order_not_lba_order`

The forensic ranges reach `fill_base_key_gaps` in `IndividualSegment.tbl`
RECORD order, which is not LBA order — the table is a list of segments, and
nothing in `aacs::segment` sorts it. The gap walk is a single forward sweep
(`cur = cur.max(ce)`), so it is only correct on cuts in ascending order;
that is what `c.sort_unstable()` is for.

Every existing gap-fill case happens to pass its cuts already sorted, so the
sort is unconstrained by them. Here the cuts arrive REVERSED: without the
sort the sweep takes the high cut first, jumps `cur` past it, then discards
the low cut as "already behind" — and emits a base-key fill straight over
the low forensic segment. That is the silent-wrong-key shape: the forensic
LBAs end up in TWO ranges, one of them keyed with the base Unit Key.

## `RecordingSource`

A `SectorSource` that records every `(lba, count)` it is asked for and
serves a caller-chosen aligned unit, so the probe SPREAD of
`sample_encrypted_units` is observable directly.

## `sample_encrypted_units_probes_eight_aligned_units_spread_across_the_extent`

`sample_encrypted_units` is the evidence every CPS-unit decision rests on,
so WHICH units it reads matters: 8 probes at `total * p / 9` for p in 1..=8,
each a whole aligned unit (3 sectors) measured from the extent's own start.

Pinned exactly. A probe count, a divisor or a `p` range that drifts moves
the sample set — clustering probes at one end of a 20-minute clip, where an
authored-bad or padding region can make an extent look unopenable — and
each of those is an independently reachable mutation of this arithmetic.

## `sample_encrypted_units_drops_clear_units_and_reads_nothing_below_one_unit`

Only genuinely AACS-encrypted units come back — that is the whole contract
that lets a caller treat a decrypt-to-clean as proof of the key. A clear
(unencrypted) extent yields NO samples, which is what makes `pick_pool_slot`
return `None` and the caller inherit rather than fail loud.

## `pick_pool_slot_honours_the_caller_s_slot_list_and_its_order`

`pick_pool_slot` answers "which of THESE slots, in THIS order, opens the
extent" — and its two callers pass different slot lists for a reason: the
multi-CPS path offers the whole pool, the FMTS gap fill only the BASE keys
(offering a forensic index key there would key a whole extent with it).

So both the RESTRICTION and the ORDER are load-bearing, and neither is
implied by "some slot matched". Two samples open under two different pool
slots here, so the answer is decided purely by which slot the caller listed
first — a `find` that ignored `slots` order, or that scanned the pool
instead, would return the same value for both directions.

## `extents_overlap_is_half_open_at_both_ends`

Extents are `[start_lba, start_lba + sector_count)` — half open. This
decides whether a title "reads the forensic clip", i.e. whether
`resolve_fmts_key_map` resolves index keys at all or returns `Ok(None)` and
leaves the title on the base-Unit-Key path.

Both directions are wrong in a way that does not fail loudly: an inclusive
end makes a title that merely ABUTS the forensic clip resolve (and pay a
key-service round trip) for content it does not read, while a stricter test
makes a title that shares exactly one sector fall through to a
base-key-only map and silently garble its forensic units.

## `fmts_resolve_err_with`

Resolve the synthetic FMTS disc with `extra` bogus records appended to its
segment table, and return the error text.

## `fmts_baseline_table_resolves_so_the_unmappable_record_tests_mean_something`

Control: the SAME resolve with no extra records succeeds. Without this the
four tests below could be passing for any reason at all — a fixture that
never reaches the range builder would `expect_err` just as happily.

## `fmts_inverted_segment_record_aborts_rather_than_base_keying_the_hole`

Arm 1 — an INVERTED record (`start_spn > end_spn`). `end_byte - 1 -
start_byte` would underflow, so the record is refused; the tally is what
turns that refusal into a loud failure instead of a base-keyed hole.

## `fmts_record_with_no_index_key_aborts_rather_than_base_keying_the_hole`

Arm 2 — a record whose forensic INDEX has no key. The synthetic source
returns two index keys, so tags 1 and 2 have pool slots and tag 3 has none.
On a real disc this is a table that outruns the key set the service
returned — precisely the case where guessing a key is worst.

## `fmts_record_running_past_the_clips_end_aborts_rather_than_base_keying_it`

Arm 3 — a record whose START is addressable within the clip (so
`filter_addressable_segments` keeps it) but whose END runs past the clip's
last byte. Only the second `clip_byte_to_lba` fails, which is why the
filter upstream cannot stand in for this check.

## `fmts_record_off_the_aligned_unit_grid_aborts_rather_than_spanning_wrongly`

Arm 4 — a record whose LBA span is not one contiguous run: `b - a` counts
SECTOR crossings while `(end_byte - 1 - start_byte) / 2048` counts the
span's own length in sectors, and the two disagree exactly when the record
is not aligned to the aligned-unit grid the forensic interleave is defined
on (or when it straddles a clip extent boundary).

A structurally valid forensic segment cannot hit this: the interleave is
per 6144-byte aligned unit, so a real record starts and ends on a
32-packet boundary and the two counts agree. `start_spn = 10` does not —
clip byte 1920 is mid-sector — so the span is refused.

## `multi_cps_cache_hit_still_feeds_the_next_extents_inheritance`

The multi-CPS loop's inheritance chain has to run THROUGH a cache hit. An
extent served from the memo never re-samples, so its index reaches the next
extent only because the hit arm carries it into `last_idx`; without that, a
following extent with no sampleable ciphertext inherits whatever the loop
started at (slot 0) instead of its neighbour's key.

That is silent: an unsampleable extent produces no error either way, so the
map simply keys those LBAs to the wrong CPS unit and the mux decrypts them
to garbage with `lost_bytes == 0`. The existing shared-extent test resolves
a SINGLE-extent title through the cache, so nothing downstream of the hit
is observed; here the clear extent is deliberately placed AFTER the hit.

## `fmts_multi_cps_gap_fill_carries_the_preceding_extents_cps_unit_to_a_clear_tail`

The FMTS gap fill on a MULTI-CPS disc runs its own extent loop, with its own
inheritance chain (`base_slot_for_extent`'s `last_idx`). A title whose last
extent has no sampleable ciphertext — a clear/nav tail, which is exactly the
case that cannot fail loudly — must take the CPS unit its neighbour is in,
not `base_slots[0]`.

Slot 0 here is CPS unit 1's key and the neighbour is CPS unit 2, so losing
the carry keys the tail with the wrong unit's key and decrypts it to
garbage with no error at all.

## `fmts_multi_cps_gap_fill_samples_each_extent_once_per_disc`

The FMTS gap fill samples extents off the LIVE DRIVE through the same
per-disc `CpsUnitCache` the multi-CPS path uses — 8 random 6144-byte reads
per extent, ~200 ms of seek apiece. Every input to that decision is in the
cache key, so a second title over the same extents must cost ZERO further
content reads.

The index-key memo alone does not deliver that: it short-circuits the
anchor and phase probes but the gap-fill loop still runs, and on a
multi-CPS disc it re-samples every extent unless `base_slot_for_extent`
banked its verdict.

## `fmts_stop_during_the_udf_walk_touches_no_content_sector`

Stop lands while the UDF walk is still running — before the anchor loop.
The anchor loop's own poll must catch it, so the drive is never asked for a
single CONTENT sector. Without that poll the entry poll has already passed
and the next one is inside the phase loop, so the full anchor batch (two
`MIN_SAMPLE_UNITS` phase reads plus a key-service round trip) is issued to
a drive the operator has already stopped.

## `fmts_stop_during_the_anchor_batch_stops_before_the_phase_probes`

Stop lands with the drive already working, during the anchor batch. The
PHASE loop's poll must catch it before probing index 1's parity —
otherwise every index in the set is probed (`MAX_ANCHOR_ATTEMPTS` segments
× `MIN_SAMPLE_UNITS` × 2 parities each) after the operator said stop.
