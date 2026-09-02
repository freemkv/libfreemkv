# keysource.rs — extended notes

Overflow rationale relocated from `src/keysource.rs` doc comments by the
comment-guard (prose caps: 8 lines for pub-item rustdoc, 3 lines for internal
comments). Each section below is pointed to by a short `//`/`///` comment at
the corresponding site in the source.

## Module overview — ownership split

libfreemkv still OWNS the crypto: the boil-down primitives and the AES live
here. A source owns only PATH ORCHESTRATION — deciding which primitive to
call with what input for the material it happens to hold. Source
implementations are published in the companion `freemkv-keysources` crate,
keeping key *policy* (which store, which order, online vs local) out of the
library. It knows what material it holds (a DK / MK / VUK / pre-decrypted UK)
and what it must fetch from the disc (VID, MKB, encrypted title keys, content
samples) to get there.

## `MIN_SAMPLE_UNITS` — why this many, why here

The key service identifies a key by which of the submitted units it decrypts,
so too few samples — especially on FMTS, where a segment interleaves several
variants at the unit level — can return a key that matches an incidental unit
rather than the one asked about (a false positive). This many distinct units
make the request unambiguous.

Canonical here (the base crate) so BOTH consumers agree on one value: the
online source in `freemkv-keysources` (which refuses to send an under-sampled
request) re-exports it, and libfreemkv's own FMTS forensic query
([`crate::mux`]) sizes its per-segment batch by it. Layering forbids the
reverse import (keysources depends on libfreemkv, not vice versa), so the
value lives at the lower layer both share.

## `DecodeSampleSet` — parse, don't validate

"Parse, don't validate": the only constructor, `DecodeSampleSet::new`, returns
`None` for an under-sized slice, so an online key request simply *cannot be
built* from too few samples. The runtime `len() < MIN_SAMPLE_UNITS` check that
used to live at the request site (and was silently forgotten by an
under-sampling caller, reading as "key service down") becomes a compile-time
obligation: a request builder that takes `&DecodeSampleSet` can never receive
an unchecked `Vec`.

The *count* enforced here is a runtime property of the disc (how many
encrypted units it yields); the *requested* count is a caller-side
compile-time constant that callers pin to `MIN_SAMPLE_UNITS` (see e.g.
autorip's `SAMPLE_UNITS`). Together the two make under-sampling
unrepresentable at the request boundary.

The wrapped samples are on-disc AACS ciphertext — the same bytes the sibling
`DiscInputs::samples` redacts as key MATERIAL — so `Debug` is hand-written and
redacting.

## `DecodeSampleSet`'s hand-written `Debug`

A derived `Debug` dumped every wrapped sample verbatim: a `DecodeSampleSet`
carries at least `MIN_SAMPLE_UNITS` 6144-byte aligned units (>= 49 KiB, in
practice multi-MB) of AACS ciphertext plus each unit's clear 16-byte
derivation seed, so one `tracing::debug!("{set:?}")` on a failed `/decode`
request — or an `assert_eq!` whose panic message formats it — wrote all of it
to the log that gets attached to a bug report. Same policy and same shape as
`DiscInputs`'s impl.

## `DiscInputs`'s hand-written `Debug`

Redacting `Debug`, per the policy `aacs::types` documents (and which
`aacs::types::Vid` already applies to this very Volume ID). `DiscInputs` is
public and returned by `crate::Disc::inputs`, so a consumer's
`tracing::debug!("{inputs:?}")` used to print the Volume ID, the whole
`Unit_Key_RO.inf` (the encrypted title keys), the entire MKB and every
ciphertext sample verbatim into a log that ends up attached to a bug report.
Only non-secret identity and shape (presence, lengths) is printed.

## `DiscInputsCtx::new` — parse-failure policy

A present-but-malformed `unit_key_ro` (truncated / wrong magic / wrong
stride) parses to an empty key set, so a later `enc_title_keys` returns
`Ok(&[])` indistinguishably from a disc that legitimately has no title
keys — the parse failure is swallowed here, not surfaced as an error.

## `KeySource` trait — the two resolve operations

Returning an empty `Vec` means "no key for this disc from this source"; an
`Err` means the source itself failed (I/O, parse, network). The caller
(`resolve_and_apply`) tries each source in order and validates the returned
keys against real ciphertext before committing them, so a wrong key from one
source transparently falls through to the next.

Two explicit resolve operations, one per key kind — never one overloaded call
whose meaning depends on how many keys came back:
* `get_unit_keys` — the disc's base per-CPS-unit Unit Keys (index space =
  CPS-unit number). The common path for every disc.
* `get_fmts_indexes` — the AACS 2.1 forensic index keys (index space =
  forensic index 1..N). Defaults to empty: a source with no forensic material
  opts out, and only an FMTS disc ever asks.

What each source must do to answer is the source's own business: a keydb keys
on `disc_hash` and reads no samples; the online source submits the ctx's
content samples (a base batch for `get_unit_keys`, an index-1 anchor batch
for `get_fmts_indexes`) to the key service.

## `resolve_and_apply_traced` — one-shot semantics and CPS-unit numbering

One-shot per source: each source's `KeySource::get_unit_keys` is called
exactly once with a `DiscInputsCtx` over `inputs`. Non-empty Unit Keys are
mapped to terminal `Key::Unit`s and applied via `crate::Disc::decrypt_with`,
which validates them against `inputs.samples` and only mutates the disc on
success — so a wrong/partial key set is rejected and the loop continues.

CPS-unit numbering: a source returns Unit Keys carrying the POSITIONAL index
from `crate::aacs::derive::decrypt_unit_key`; the library's canonical
CPS-unit number is `position + 1` (matching
`crate::aacs::inf::parse_unit_key_ro`'s `(i + 1)`), so the committed
`AacsState.unit_keys` is byte-identical to the library-resolved path.

The NUMBER itself is not what descramble indexes by — but the ORDER is
load-bearing, so a source must return its keys in CPS-unit order. Trial
decrypt-and-check was deliberately deleted (see `crate::decrypt::AacsKeyMap`:
decryption is driven by the disc's CPS-unit / FMTS-segment structure, "never
by trial-decrypt-and-check per unit"), and `decrypt_sectors_mapped` indexes
the committed pool POSITIONALLY — `unit_keys[key_idx].1`, where `key_idx` is
a POSITION in the Vec a source returned, recorded by
`resolve_mux_key_map_cached` / `resolve_fmts_key_map`. Return the same keys
in a different order and every `AacsKeyMap` points at the wrong key: the
whole title decrypts under a neighbour's key, or a forensic range trips the
`is_clean` net into `DecryptFailed`. (The doc used to say the number "is
cosmetic for descramble (the decrypt path strips it and tries every key)",
which is what the DELETED trial-decrypt path did; the only place that still
tries every key is `Disc::decrypt_with`'s sample VALIDATION, which does not
descramble content.)

## `fetch_unit_keys` — the two callers

This is exactly what both paths do — only the samples differ:
* at disc open, `ctx` carries reachable-content samples → resolves the
  up-front CPS units (the common one),
* in the read, on a decrypt miss, `ctx` carries the FAILING unit's ciphertext
  → resolves the CPS unit that wasn't sampled up front.

Same sources, same call; there is no separate "fetch". Unlike
`resolve_and_apply` this does not validate/commit to a disc — the read's
decorator re-decrypts with the returned keys, which is the validation.

## `FetchOutcome` — the error-signal struct

Whether a driver run resolved keys, and — when it did NOT — whether the miss
was a genuine "no source holds this key" (`errored == false`) or at least one
source FAILED (`errored == true`, e.g. a network source was unreachable). The
distinction gates negative-result memoization: an empty-because-absent
result is safe to cache, an empty-because-a-source-was-down result is
transient and must NOT be cached (the key may resolve once the source
recovers).

## `drive_unit_keys`

`fetch_unit_keys` plus the error signal: drive `sources` in order, return the
first source's non-empty Unit Keys, and flag whether any source that failed
to answer did so with an `Err` (a source failure) rather than an empty `Ok`
(genuine absence — see `KeySource::get_unit_keys`).

## `key_fetch` — one builder, memoization policy

One builder, used by every read path (sweep / patch / mux) and every
consumer (CLI, autorip) — neither application contains the fetch logic, only
its key-source config. Cheap to clone; build once, clone into each read
path. `make_sources` is invoked per fetch (the cold path) so the resolver
stays `Send + Sync` without requiring `KeySource: Send`.

Each operation is memoized per sample-batch fingerprint (keys are disc-level,
so repeats hit cache). A genuinely-empty reply is cached, but a source
FAILURE is not — a transient miss (see `FetchOutcome::errored`).

## `read_encrypted_units` — why here, and the CPI selection rule

Lives in the library, not a key-source crate: reading the disc and carving
AACS units is decryption *mechanism* (unit geometry anchored at each
extent's `start_lba`), which the library owns. A key source is *handed*
these bytes via `DiscInputs.samples`; it never reads the disc itself.

"Encrypted" is decided by `crate::aacs::content::aacs_unit_encrypted` — the
AACS Copy Permission Indicator (CPI) in the top 2 bits of byte 0, the
spec-correct signal (`buf[0] & 0xc0`). NOT the `is_clean` TS-sync heuristic:
a unit lacking clean TS syncs does not imply encryption (an FMTS variant
frame or an odd clear unit can lack syncs yet be unencrypted), and a clear
unit sent to a key server yields nothing to validate against — the "0
encrypted units" rejection. A clip opens with clear navigation units
(PAT/PMT, menus) whose CPI is clear; only CPI-flagged content units are
collected — probing several points spread across EACH extent so a title
whose encrypted body starts late (or whose midpoint lands in clear nav)
still yields samples. CPI is read at each extent's `start_lba`
(clip-file-anchored), so byte 0 is a real unit start and the flag is
meaningful.

## Test doc overflow

### `key_source_host_certs_defaults_to_empty`
Spec: a source holding no cert returns the empty vec; the `mkb` param is
forward-looking and the default ignores it. Mutation: a default returning a
non-empty vec would inject phantom certs into the OEM handshake.

### `errored_empty_is_not_cached_and_retries_when_source_recovers`
A transient source outage must NOT be memoized as a permanent "no key": a
fingerprint whose first fetch failed because the source errored must be
re-asked, and once the source recovers the key resolves. Regression guard
for the negative-result memoization fix — caching the errored empty would
permanently drop a recoverable unit for the rest of the op.

### `key_fetch_routes_unit_and_fmts_to_distinct_source_methods`
The two `KeyFetch` operations route to the two DISTINCT trait methods:
`unit_keys` drives `get_unit_keys`, `fmts_indexes` drives
`get_fmts_indexes`. A source that returns different keys per method proves
the seam no longer collapses "1 base key" and "the forensic set" into one
overloaded call — the operation, not the return length, decides which.

### `read_encrypted_units_finds_scrambled_content_off_the_midpoint`
#4 regression: encrypted content NOT at the extent midpoint (a
late-starting feature, or a midpoint landing in clear nav) must still be
sampled — empty samples make `decrypt_with` skip wrong-key validation. The
old midpoint-and-forward sampler returned empty; the probe-spread finds the
early scrambled band.

### `read_encrypted_units_selects_by_cpi_not_ts_sync`
DISCRIMINATING: selection is by the AACS CPI (byte 0), NOT the TS-sync
clarity heuristic. Half the units lack TS syncs but are CPI-CLEAR
(`byte0 & 0xC0 == 0`) — genuinely UNencrypted units that merely lack TS
syncs; the old sampler collected these and the key server rejected the POST
as "0 encrypted units". `read_encrypted_units` must skip them and return
ONLY CPI-flagged units. A regression to selecting by TS-sync clarity would
collect the CPI-clear units too and fail the `& 0xC0` assertion.

### `disc_inputs_ctx_parses_unit_keys_at_the_version_stride`
Audit #5 — a DISCRIMINATING test for the version→stride fix. A 2-key
`Unit_Key_RO.inf` whose SECOND key sits at the V20 (64-byte) offset; a V10
(48-byte) parse reads a DIFFERENT region. Confirms `DiscInputsCtx` parses at
the stride for `inputs.version` — a swapped `from_major` branch or a
hardcoded stride (the exact bug 1.2.0 fixes) would fail this, where the
prior single-key fixtures passed regardless of stride.

### `disc_inputs_debug_is_redacted`
`DiscInputs` is public and returned by `Disc::inputs`, so any consumer's
`tracing::debug!("{inputs:?}")` prints it. A derived `Debug` printed the
Volume ID (the value `aacs::types::Vid` deliberately renders as
`Vid(<redacted>)`), the whole `Unit_Key_RO.inf` (the encrypted title keys),
the entire MKB and every ciphertext sample verbatim. Sentinel byte 0xD5 =
decimal 213, matching `aacs::types::redaction_tests`. Mutation guard:
restoring `#[derive(Debug)]` fails this.

### `decode_sample_set_debug_is_redacted`
`DecodeSampleSet` is public and wraps the SAME on-disc ciphertext the
sibling `DiscInputs` redacts, so a derived `Debug` dumped >= MIN_SAMPLE_UNITS
x 6144 bytes of verbatim AACS ciphertext (plus every unit's clear 16-byte
derivation seed) into any log that formatted it. Sentinel byte 0xD5 =
decimal 213, matching `aacs::types::redaction_tests` and the `DiscInputs`
test above. Mutation guard: restoring `#[derive(Debug)]` fails this.
