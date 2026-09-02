# decrypt.rs — relocated rationale

Overflow prose that would not fit under `ci/comment-guard.py`'s caps,
moved here per file with a `// See docs/decrypt.md — <topic>` pointer
left at the original site.

## Parallel AACS decrypt

Each AACS aligned unit (6144 bytes) is decrypted INDEPENDENTLY of every
other unit — per-unit key derivation from the unit_key plus the unit's
own first-16-byte header. There is no cross-unit dependency, so a
buffer of N units can be decrypted on N threads in parallel via a
persistent rayon thread pool.

Small buffers (< `PARALLEL_MIN_UNITS` units) fall through to the serial
path to avoid pool dispatch overhead beating the per-unit AES work.

## Thread-count configuration — three layers

Resolution order (highest wins):
1. The most recent `set_decrypt_threads` call with `n > 0`. Calling
   this *replaces* the live thread pool — useful for a settings-page
   slider in a long-running daemon.
2. `FREEMKV_THREADS` env var, if set and `> 0`. Single knob covering
   decrypt today, intended to also drive any future input-side /
   output-side worker pools.
3. Default: all available cores. Algorithm optimisation comes first —
   we measure single-thread performance to find serial bottlenecks
   before throwing parallelism at it — but once a pool is engaged we
   use the whole box. Hard cap at `MAX_THREADS` (rayon stack memory).

## DECRYPT_POOL

Current rayon pool. `RwLock<Option<Arc<...>>>` so that
`set_decrypt_threads` can swap the pool out without leaking the old
one and without blocking ongoing decrypt work (in-flight calls hold an
`Arc` clone via `decrypt_pool` and finish on the old pool; new calls
pick up the new pool).

## decrypt_pool()

Get (or lazily build) the active rayon thread pool. Returns an `Arc`
so in-flight work survives a concurrent `set_decrypt_threads` swap.

Returns `None` if the pool cannot be built (e.g. the OS refuses the
worker threads under a pid/thread limit). The caller falls back to the
serial decrypt path — library code never panics here.

## unit_is_our_phase

Does this unit belong to the phase we hold the key for?

An FMTS forensic segment interleaves two variants at the unit level:
the disc carries both, and we hold the key for exactly one parity.
Decrypting the alternate half with our key produces garbage; leaving
it as ciphertext is correct, because the muxer drops untouched
ciphertext cleanly.

`Phase::All` means the whole range is ours (the non-forensic case).

This lived inline inside `apply_aacs_map`'s per-unit closure, where
nothing could reach it: a mutation run flipped the `-` to `+` and the
`/` to `*` in the index arithmetic and every test still passed.
Getting either wrong silently decrypts the wrong half of a forensic
segment.

## AacsKeyMap (proactive AACS key-selection map)

Which held unit key decrypts each LBA of a title's encrypted content,
decided ONCE before mux from the disc's CPS-unit (and, later, FMTS
segment) structure — never by trial-decrypt-and-check per unit at mux
time.

This is the pivot that ends the mux "key-server storm": the old path
decrypts a unit, checks whether the plaintext looks like clean MPEG-TS,
and — because authored-bad content never reaches that bar — concludes
"wrong key, fetch a fresh one" and re-asks the key service for units it
already holds the correct key for. There is NO per-unit byte pattern
that separates "correctly decrypted but authored-bad" from "still
encrypted", so that check is unanswerable. The map removes the
question: we resolve one key per CPS unit / segment up front (see
`resolve_mux_key_map`), record which LBA ranges each covers, and at mux
time simply "decrypt this LBA with key K" and trust it — bad TS is the
muxer's concern, exactly as for a physically-read clear disc.

Ranges are `[start_lba, end_lba)` → index into the `Aacs { unit_keys }`
pool, sorted and disjoint. The map is a POSITIVE list: an LBA in no
range is passed through untouched (no default key). How a single-CPS
disc is mapped depends on the caller: the whole-disc EXTRACT path uses
one blanket range `(0, u32::MAX, 0)` so every encrypted unit — parsed
title or orphan clip — resolves to key 0; the per-title MUX/sweep path
(`resolve_mux_key_map` → `content_map`) maps only the title's own
extents, so an orphan clip outside them is left as pass-through. Either
way, clear nav/filesystem sectors (encrypted-flag off) pass through.

## AacsKeyMap::read_plan

Build the FMTS **read plan**: the title's aligned units filtered down
to only the units this rip must actually read — every default / CPS
unit, plus, inside each forensic segment, ONLY our-phase (`Phase::Even`
/ `Phase::Odd`) units. The alternate-phase units are a different device
group's variant: a licensed player never reads them, and neither do we.
They are omitted from the plan entirely, so they are never fetched,
decrypted, or handed to the demux — the demux therefore sees one
gapless our-variant stream, with no ciphertext to trip a
concealed-gap resync (the old behaviour that dropped good frames
around every segment).

`extents` are the title's clip extents (unit-aligned in the interior; a
shorter tail is ordinary content and always kept). `unit_sectors` is
the AACS aligned-unit size in sectors (3). Contiguous kept units
coalesce into as few extents as possible so the producer still issues
large sequential reads across default content; only inside a ~480 KB
forensic segment do reads become unit-granular (every other unit). A
map with no forensic (Even/Odd) range returns `extents` unchanged — the
common disc is not touched.

The parity test is byte-identical to the decrypt hot loop
(`(unit_lba - range_start) / unit_sectors`), so a unit kept here is
exactly a unit `decrypt_sectors_mapped` would open, and vice-versa.

## apply_aacs_map

AACS scheme step: apply `map`'s per-unit keys to `buf`, in-place.
`base_lba` is the absolute LBA of `buf`'s first sector; each aligned
unit (3 sectors) is decrypted with the key the map assigns to its LBA.
There is NO key trial and NO `is_clean` verdict: the map already
decided the key from disc structure, so this applies it and moves on —
a unit that decrypts to authored-bad TS passes through for the muxer to
drop, never re-fetched.

A SCHEME, not a policy. It reports what it could not open by returning
`Err(DecryptFailed)`; the decision that an unopenable unit must never
be emitted belongs to `decrypt_span`, which is the one place that
decides it for every scheme. A map index outside the held pool is
likewise a fail-loud `Error::DecryptFailed`: the resolver's job is to
guarantee every selectable index is present, so a gap here is a
resolver bug, not silent loss.

## decrypt_sectors — CSS / clear detail

For CSS: descrambles per 2048-byte sector, self-cracking the title key
from the data (no external input). For `None`: a no-op. For AACS:
**always** returns `Err(DecryptFailed)` — AACS decrypts exclusively
through the resolved key map (`decrypt_sectors_mapped`), which keys
every content unit up front and fails at RESOLVE time when a key is
missing. Reaching this arm with AACS keys means a reader was built
without installing its map (a bug), so it fails loud rather than apply
a guessed key.

`unit_key_idx` is a legacy parameter kept so the CSS / `None` wrapper
signature stays stable; it is ignored (the CSS arm self-gates on its
per-sector scramble flag). Returns `Err` if decryption was expected but
impossible; never produces silently corrupted output. The `usize`
return is a legacy unverified-byte count that is always `0` for the CSS
/ `None` arms.

## decrypt_span — orchestrator rationale

THE decrypt orchestrator. Every path into this crate's decryption goes
through here.

How a disc decrypts is one process — resolve a key for this span,
apply it, and refuse if no key can be proven. Only the resolve-and-apply
step is scheme-specific. This function owns the loop and the refusal;
the schemes below supply only what genuinely differs between AACS, CSS
and clear media.

That split exists because its absence caused six separate defects in
one release. There used to be TWO top-level paths — this one for CSS
and clear, and a wholly separate `decrypt_sectors_mapped` for AACS
whose arm here was a bare `return Err` stub — so each scheme decided
its own answer to "there is no key for these bytes" and nothing held
them to the same one. CSS drifted to descrambling with a key it had
just proven stale; the mapped path drifted to passing an unkeyable
encrypted unit through as ciphertext. Both looked like success to the
caller.

Adding a scheme means adding an arm here, which means answering the
refusal question. That is the point.

## Tests — relocated rationale

### a_forensic_read_plan_drops_units_the_full_extents_include

`DecryptKeys::None` is a no-op even with a content map + scrambled
bytes. A forensic map's read plan is NOT the extents it was given —
and that is the precondition the mux's provenance guard keys on.

A clip's feed span is measured over the FULL extents at scan time,
while the mux reads this reduced plan, so the byte offsets stamped on
frames and the offsets recorded in the spans describe different
streams. The deficit accumulates, so every frame after the first
segment resolves to an earlier clip than it came from. The spans still
tile each other, so the tiling check cannot see it; the mux compares
the plan against the full extents instead and stops trusting
provenance when they differ.

### content_gate_css_keys_is_noop

CSS ignores the content gate (it lives in the AACS arm) and always
reports `0` — confirming the gate is a no-op for CSS and the read
stays scheme-agnostic (the litmus test: adding CSS verify touches only
the CSS arm, never the read).

### content_gate_css_actually_descrambles_the_buffer

`decrypt_sectors_in_content` is the entry point `DecryptingSectorSource`
dispatches to whenever a content map is installed
(`sector/decrypting.rs` line ~211), so it is on the live read path for
every mapped rip. It must actually DECRYPT. The two `_is_noop` tests
above only assert its `usize` return is `0` — which is what a body
replaced by `Ok(0)` also returns, so neither one constrains it at all.

Here a genuinely scrambled CSS sector goes in and the CONSTRUCTED
plaintext must come out. Anything that skips `css::descramble_region` —
including a body that just reports `Ok(0)` — leaves ciphertext in the
buffer and the caller muxes scrambled MPEG at exit 0.

Expected bytes come from the plaintext this test built BEFORE
scrambling (CSS scrambles only 0x80..2048; the header stays clear), not
from re-running any descramble routine.

### content_gate_aacs_keys_fail_loud_not_ok_zero

The AACS arm of the same entry point must fail LOUD. Under the
keymap-only model AACS decrypts exclusively through
`decrypt_sectors_mapped`; reaching this wrapper with AACS keys means a
reader was built without installing its key map, and continuing would
hand the caller ciphertext under an `Ok`. `DecryptFailed` is the
correct verdict per the function's own contract — it must not be
softened into a success with a zero count.

### css_region_change_recracks_the_title_key

CHARACTERIZATION (recovery refactor safety net): the CSS arm's
per-region re-crack (the `title_key` cache is stale for a new VOB
region → restore ciphertext, `crack_title_key` this sector,
re-descramble). Two crackable sectors scrambled under DIFFERENT keys
sit back-to-back; the cache is primed to the FIRST key. Sector 0 rides
the cache (crib matches); sector 1 must trip the crib mismatch and
re-crack to its own key. Both must land correct plaintext, and the
cache must end on region 1's key.

This behaviour currently lives inline in `decrypt_sectors` (the `Css`
arm). It is the delicate logic the recovery refactor will move to the
input-stream seam, so it must stay green byte-for-byte across that
move.

### aacs_scrambled_trailing_partial_is_rejected

Whole leading unit plus a SCRAMBLED trailing partial that is FLAGGED
encrypted in its clear seed (the malformed danger case): an encrypted
unit split across an extent boundary cannot be CBC-decrypted
standalone. The mapped decrypt must fail loud with `DecryptFailed`
rather than emit the ciphertext partial as clear. Exercises the real
shipping path (`decrypt_sectors_mapped`) and its trailing-partial
guard.

### aacs_clear_trailing_partial_passes_through

A CLEAR trailing partial (encrypted flag NOT set) is a legitimate
content tail and must pass through, never trip the guard above.

"Passes through" means byte-for-byte unchanged, not merely `Ok`.
Asserting only `is_ok()` let a mutant that corrupts the clear partial
while still returning `Ok` pass — which is the whole failure this test
names. Mutation: XOR any byte of the tail before returning -> the
snapshot comparison fails.

### none_keys_is_noop

`DecryptKeys::None` is a pure no-op: the buffer must be returned
byte-for-byte unchanged with Ok, regardless of content (even content
that looks scrambled).

Grounding: the `DecryptKeys::None => {}` match arm does nothing.
Mutation: replace the empty arm with a call that mutates buf -> the
unchanged assert fails.

### is_encrypted_matches_variant

`is_encrypted` reflects the variant: None -> false, Css/Aacs -> true.

Grounding: `!matches!(self, DecryptKeys::None)`.
Mutation: invert the `!` -> None reports true, this fails.

### css_descrambles_with_title_key

The CSS path descrambles each 2048-byte sector with the title key. A
scrambled sector run through decrypt_sectors must come back to its
plaintext body (keystream XOR is involutive), proving the title key is
actually applied.

Grounding: `DecryptKeys::Css { title_key } => for chunk in
buf.chunks_mut(2048) { descramble_sector(title_key, chunk) }`.
Mutation: change `chunks_mut(2048)` to `chunks_mut(2049)` or pass a
fixed wrong key -> the body no longer matches the plaintext.

### css_processes_every_sector_in_buffer

The CSS path processes EACH 2048-byte sector independently in a
multi-sector buffer. Two scrambled sectors (with different seeds) in
one buffer must both round-trip — pinning that the loop steps by 2048
and applies the key to every sector, not just the first.

Grounding: `for chunk in buf.chunks_mut(2048)`.
Mutation: change the loop to descramble only the first chunk (e.g.
`.next()`) -> the second sector stays scrambled, assert fails.

### css_rekeys_when_title_key_region_changes

CSS title keys are per-VTS/VOB region: a real disc holds DIFFERENT keys
for different regions and the only way to get each is to crack it. The
decrypt path must re-crack when the cached key stops descrambling (its
crib no longer reappears at 0x80) instead of blindly applying one key
across a region boundary — the bug that pixelated every freemkv DVD
rip.

Two sectors scrambled under DIFFERENT keys, cache primed to ONLY the
first (exactly what the one-shot scan crack leaves). Sector 0 validates
+ descrambles with the cached key; sector 1's cached-key descramble
fails the crib, so the path re-cracks sector 1's own key and recovers
its plaintext. Before the fix (blind single-key apply) sector 1 was
garbage.

Grounding: the CSS arm's `attack_crib` → `chunk[0x80..] != crib` →
`crack_title_key` → `*title_key = fresh` rekey.
Mutation: drop the rekey branch (apply the cached key always) →
sector 1's body no longer matches its plaintext; this fails.

### css_leaves_clear_sector_unchanged

The CSS path leaves UNSCRAMBLED sectors (flag clear) byte-for-byte
untouched — descramble_sector early-returns on a zero flag. A clear
sector mixed into the buffer must not be corrupted.

Grounding: descramble_sector returns immediately when
`(sector[0x14] >> 4) & 0x03 == 0`.
Mutation: remove that early return in lfsr.rs -> a clear sector would
be XORed with a keystream and change; this fails.

### css_empty_buffer_is_ok

CSS decrypt always returns Ok (it cannot fail — descrambling is XOR,
no key validity check), even for an empty buffer.

Grounding: the CSS arm has no `return Err` path; `chunks_mut` over an
empty slice is a no-op; the function ends `Ok(())`.
Mutation: make the CSS arm return Err -> this fails.

### aacs_mapped_out_of_range_key_idx_errors

A map that selects a key index OUTSIDE the held pool must fail loud
with DecryptFailed — never silently apply a wrong key or pass
ciphertext through. This validates `decrypt_sectors_mapped`'s up-front
`key_indices()` bounds check (the real shipping AACS decrypt path).

Mutation: drop the `unit_keys.get(idx).is_none()` guard → the
out-of-range index would not error; this fails.

### aacs_via_unmapped_decrypt_sectors_fails_loud

SAFETY NET: reaching the CSS/`None` wrapper (`decrypt_sectors`) with
AACS keys means a reader was built with no map — a bug. It must fail
loud, never apply a guessed key. (AACS decrypts exclusively via
`decrypt_sectors_mapped`.)

### read_plan_forensic_reads_only_our_phase_units

FMTS: a forensic Even segment drops exactly its alternate (odd) units
from the read plan — they are never fetched — while default content on
either side stays one coalesced sequential run. The kept units are
byte-identical to the ones the decrypt hot loop opens.

### read_plan_phase_parity_is_measured_from_an_unaligned_range_start

A forensic range does NOT start on an aligned-unit boundary. Its start
LBA comes from a source-packet number — `start_spn * 192` put through
`clip_byte_to_lba` (`mux/resolve.rs`) — and 192-byte packets have no
relationship to the 3-sector aligned unit, so `range_start % 3` is
whatever the disc says.

That makes `unit_ix = (lba - range_start) / us` load-bearing in both of
its operations, and the existing coverage used a range starting exactly
on the extent's first unit, where several wrong formulas agree with the
right one by arithmetic accident.

Getting the parity wrong is not a crash. It reads and decrypts the
ALTERNATE variant's half of a forensic segment: the units this disc's
key does not open decrypt to garbage, and the units it does open are
skipped. AACS 2.1 forensic marking is exactly the mechanism that makes
the two halves different, so a phase inversion is silent — it produces
a full-length rip carrying the wrong variant.

### read_plan_gates_the_last_whole_unit_of_an_extent_not_just_the_remnant

An extent whose last whole unit is an alternate-phase unit must still
drop it. The tail guard exists for a REMNANT shorter than a unit —
bytes with no following unit to desync — and an extent ending exactly
on a unit boundary has no remnant at all.

With the guard widened to `remaining <= us`, the final unit of every
extent bypasses the phase gate and is read unconditionally. On a
forensic segment that lands at an extent end, that is one
alternate-variant unit pulled into the rip and decrypted with a key
that does not open it.

### every_scheme_gives_the_same_verdict_when_no_key_can_be_proven

Every scheme that CANNOT prove a key answers the same way.

This is the property `decrypt_span` exists to hold. There used to be
two top-level decrypt paths — one for CSS and clear media, one for
AACS — and each decided its own answer, so they drifted apart in
opposite directions within a single release: CSS descrambled with a
key it had just proven stale, and the AACS path passed an unkeyable
encrypted unit through as ciphertext. Both reported success.

Asserting one verdict across the schemes is what makes a future
divergence a test failure rather than a silent corruption. A
per-scheme test cannot do that: each would still pass while the two
disagreed.

CSS is deliberately NOT in this list. Its title key is recovered from
the data, sector by sector, by a heuristic that false-positives — a
crib mismatch whose re-crack fails means the crib was wrong, not that
the key is stale, so the cached key is kept and used. Round 9 folded
CSS in here on the reasoning that "no key" should mean one thing
everywhere; that made real DVDs unrippable, and the real-media gate
caught it. Uniform policy is right for schemes that can PROVE a key
wrong. CSS cannot.

### an_encrypted_unit_outside_every_key_range_fails_instead_of_passing_through

An ENCRYPTED unit that falls outside every key-map range must fail,
not pass through as ciphertext.

"The map has no key here" and "there is nothing to decrypt here" are
different statements, and only the second makes passing the unit
through correct. On a multi-CPS disc an orphan clip — referenced by no
playlist, so in no title extent and therefore in no range — hits the
first and was treated as the second. `extract_tree` then counted those
bytes as GOOD, dropped the `.partial` suffix, and reported `complete:
true`, exit 0: a scrambled file on disk with a clean bill of health.

A CLEAR unit outside every range is the ordinary case (filesystem and
nav on a whole-disc read) and must still pass through untouched — so
this asserts both directions.

### mapped_key_selection_is_positional_so_pool_order_matters

The mapped descramble indexes the committed key pool POSITIONALLY
(`unit_keys[key_idx].1`), so the ORDER of the `Vec<UnitKey>` a
`KeySource` returns is load-bearing — it is NOT "cosmetic, the decrypt
path strips it and tries every key", as
`keysource::resolve_and_apply_traced`'s doc used to claim. Trial-decrypt
was deliberately deleted; nothing here searches the pool. Reordering
the same two keys therefore sends each range to the WRONG key: the
range that decrypted clean now fails the correct-phase `is_clean` net
loudly (or, off a forensic phase, would decrypt a whole span under a
neighbour's key). Pins the corrected doc.

### decrypt_threads_within_valid_pool_range

The default (auto) decrypt thread count is always a usable pool size:
at least 1 (a 0-thread rayon pool is invalid) and never above
MAX_THREADS (rayon stack-memory cap). This test reads the resolved
value without mutating the process-global override, so it is safe to
run in parallel with other tests.

Grounding: `cores.clamp(1, MAX_THREADS)` in the default branch;
`env.min(MAX_THREADS)` in the env branch.
Mutation: change `.clamp(1, MAX_THREADS)` to `.clamp(0, MAX_THREADS)`
on a 0-core probe (unlikely) — more robustly, change the cap to
`MAX_THREADS * 2` -> on a many-core CI box the upper-bound assert can
fail. The lower-bound (>=1) guard is the load-bearing invariant.

### phase_gate_selects_only_our_parity_of_a_forensic_segment

The FMTS phase gate picks which half of an interleaved forensic
segment we decrypt. Both the `-` and the `/` in its index arithmetic
survived a mutation run, and getting either wrong silently decrypts the
alternate variant into garbage while reporting success.

### phase_gate_does_not_panic_on_a_malformed_map

A malformed key map must not take down a long-running service. A unit
below its own range start, or a zero unit size, are both map bugs —
they must return a defined answer rather than panicking on debug
overflow or dividing by zero.

Every case here asserts the DEFINED answer, not merely the absence of a
panic. The zero-unit-size case used to be written
`assert!(unit_is_our_phase(100, 30, 0, Phase::Even) || true)`, which
accepts both answers and so pinned nothing at all: the guards could
invert and it would still pass. The answer is knowable —
`saturating_sub` gives 70, `max(1)` makes the divisor 1, unit index 70
is even — so pin it.
</content>
