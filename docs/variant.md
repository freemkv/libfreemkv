# AACS Variant chain internals — relocated rationale

Supplementary notes for `src/aacs/variant.rs` comments that exceed the
comment-guard's cap. Each section is pointed to by a short
`// See docs/variant.md — <topic>` comment at the relevant call site.

## Module status

The record layout is pinned against real variant MKBs: `C` is the
per-slot block of the `0x0c` cvalue table (indexed by the matched
subset-difference — NOT the `0x2d` head), `VARIANTS[uv]` is the `0x2d`
VARIANTS table (leading `body-16` bytes, Nonce at the `0x2d` tail), and
`VKD` is `0x2f`. Two inputs still block an end-to-end run against the
`0x86` Verify-Media-Key record: the real per-licensee KCD (see
`KEY_CORRECTION_DATA` — not coded, per-manufacturer), and a covering 2.1
Processing Key. Either one missing yields a wrong `Km` that the final
verify gate rejects, so a bad key is never emitted — only an error. (A
covering key would also confirm the last layout picks: the 16-bit `Kvn`
width and Nonce head-vs-tail.)

Two condition bits on `Kmp[15]` route off the default KCD path (Soft
Correction and Online Challenge); the chain does not model those modes
and treats such a slot as non-covering.

## Verify gate

On the classical path `walk_processing_key` gates each match on the
VERIFY_MAGIC relation, which authenticates the Processing Key. On a
variant MKB that magic does NOT hold (the walk yields a Precursor, not
the Media Key), so the authoritative gate is at the END of the chain:
the derived `Km` is verified against the MKB's Verify-Media-Key record
before it is ever returned.

## MKB records this chain selects

`REC_MEDIA_KEY_VARIANT_DATA` (`0x0c`), `REC_VARIANT_DATA_AND_NONCE`
(`0x2d`), `REC_VKD_TABLE` (`0x2f`), the subset-difference/cvalue and
verify records — canonical set in `super::mkb`.

## `KEY_CORRECTION_DATA`

**KCD is PER-LICENSEE** (per player manufacturer) — there is no single
universal value. libfreemkv compiles in no AACS key material (keydb.cfg
is the single source of truth), so this stays all-zero: the chain's
SHAPE still runs, but on a real variant disc the derivation yields a
wrong Media Key that the final Verify-Media-Key gate rejects. The
variant chain therefore cannot complete on a real disc today — a
key-acquisition gap, not a code gap. If a real per-licensee KCD is ever
available it must come from keydb.cfg, never a compiled constant.

## `is_variant_mkb`

The real AACS 2.1 Variant markers — confirmed against a live variant
MKB — are `0x2d` (Encrypted Media Key Variant Data / C) and `0x2f`
(Variant Key Data table, 65,535×16). Both are absent from non-variant
1.0/2.0 MKBs (which instead carry `0x05` host-revocation-signature and
no `0x0c`/`0x2d`/`0x2f`). The earlier `0x82`/`0x83` guess was
speculative and never appeared in any real MKB.

## `variant_data_record`

Body of the `0x2d` record: the `VARIANTS` table followed by the
trailing 16-byte `Kvn` Nonce. Measured `46_100*2 + 16 = 92_216` on one
v70 disc and `92_220` on another — in both, the leading `body.len() -
16` bytes are the big-endian `u16` `VARIANTS` table (one per
subset-difference) and the last 16 bytes are the Nonce, with NO leading
header. This does NOT hold the C used for `Kmp` — that is the per-slot
block in `0x0c` (`REC_MEDIA_KEY_VARIANT_DATA`). Both `variant_nonce`
and `variants_for_uv` read this body.

## `variant_nonce`

The Nonce-at-tail placement is consistent across both reference MKBs
(the leading `body-16` bytes form the `VARIANTS` table exactly), but
head-vs-tail is only truly pinned by running the full chain against the
`0x86` verify with a covering key. Until then a wrong nonce can only
fail that final gate, never emit a bad key.

## `variant_key_data`

Confirmed against a live variant MKB: exactly 65,535 × 16 = 1,048,560
bytes, indexed by the resolved `VKDidx`. This is disc-public data (it is
why the VKD alone buys nothing without the Media Key chain above it).

## Subset-difference walk sharing

`calc_v_mask`/`calc_pk_from_dk` (and the AES-G3 seed step) are shared
with the classical walk in `super::derive`, keeping the variant SD tree
byte-identical. `aesg3` itself is imported separately in the test
module.

## `walk_processing_key`

Walks an MKB and returns the first `(Kp, uv, cvalue)` that
`device_keys` covers, or `None` if no DK walks any uv.

This is the AACS-2.1 **variant** walk; the classical walk lives in
`super::derive::derive_media_key_and_pk_from_dk`. The two are kept
separate on purpose and select MKB records in DELIBERATELY different
order:

- cvalues: this variant walk tries record `0x0c`-then-`0x07`-then-`0x05`
  (`0x0c` is the real-disc per-uv cvalue source; `0x07`/`0x05` are
  fixture fallbacks); the classical walk tries `0x05`-then-`0x07`. On a
  variant MKB the small `0x07` Explicit-Subset-Difference record carries
  the cvalue the Precursor chain consumes, whereas a classical UHD MKB
  keeps its 1:1 cvalue table in the large `0x05` record (see the note
  on `super::derive::probe::mkb_cvalues`). They must NOT be unified to
  one order — each is correct for its own MKB shape.
- finders: this walk operates on parsed `MkbRecord`s (needed because the
  variant chain also reads `0x2d`/`0x2f`); the classical walk operates on
  raw MKB bytes. Same framing, different input type.

Consequence: do NOT route the classical DK path through this function —
on a classical MKB the `0x07`-first selection picks the wrong (or
missing) cvalue and the magic check fails, so it returns `None`.

## `variants_for_uv`

Looks up the per-slot `VARIANTS` value for the matched subset-difference
slot, keyed by the same index that selected the cvalue
(`ProcessingKeyMatch::cvalue_index`).

LAYOUT (fixed against a real 2.1 variant MKB — a v70 `MKB_RO.inf`): the
`0x2d` Encrypted-Media-Key-Variant-Data body is exactly `46_100*2 + 16 =
92_216` bytes, i.e. one **big-endian u16 `VARIANTS` entry per
subset-difference slot** (1:1 with the `0x0c` variant cvalues and the
`0x04` subset-differences), with the 16-byte per-disc Nonce packed at
the **tail** (see `variant_nonce`). So the VARIANTS table is the
leading `sd_count*2` bytes and this reads its `sd_slot_index`-th entry.

The record/field *sizing* is confirmed; the one bit still to pin against
a covering key is Nonce-head-vs-tail (both fit the size) — a wrong pick
can only yield a wrong `Km`, which the final Verify-Media-Key gate
rejects (never a silent bad key).

## `derive_media_key_variant`

The one deterministic `Kp → Km` derivation for a variant MKB. A leaked
2.1 Processing Key arrives without its subset-difference slot, so this
tries `pk` against every slot and returns the Km for the slot whose
full chain passes the MKB's Verify-Media-Key record — exactly the shape
of the classical bare-PK `super::derive::derive_media_key_from_pk`,
gated by the chain's own verify so an unverified key is never returned.

VID-free by design: the Media Key is MKB-scoped. Derive the per-disc
VUK from the returned Km with `super::derive::derive_vuk`. Deriving a
Processing Key from device keys (DK → PK) is a separate concern — walk
it first via `walk_processing_key`, then call this.

Errors: `NotVariantMkb` (caller should use the classical path),
`MkbIncomplete` (a required record is missing), or
`ProcessingKeyUnavailable` (no slot verified — `pk` does not cover this
MKB, or its slot needs the soft-correction / online path, surfaced as
`SoftCorrectionRequired` / `OnlineChallengeRequired`).

## `media_key_variant_from_kp`

Runs the variant chain from a caller-supplied Processing Key and
EXPLICIT per-slot inputs — the harness entry that tries a captured `Kp`
against known slot material, bypassing both the device-key walk and the
on-MKB `VARIANTS[uv]` lookup. The caller supplies the `0x0c` C block,
the slot's subset-difference number `uv`, and its `VARIANTS[uv]`; the
MKB supplies the Nonce, the VKD table, and the Verify-Media-Key value.

Returns `(Km, Kvu)`. The terminal Verify-Media-Key gate is identical to
`derive_media_key_variant`, so a wrong `c_block` / `uv` / `variants_uv`
returns `MediaKeyVariantError::MediaKeyVerifyFailed` rather than a bogus
key. The soft-correction / online-challenge bits on `Kmp[15]` are
classified the same way, so a slot needing an out-of-band correction
path is distinguishable from a non-matching input.

(Note the KCD caveat on `KEY_CORRECTION_DATA`: without the real
per-licensee KCD this fails the verify gate on a real disc — a
key-acquisition gap.)

## Test fixture: `synthetic_variant_setup`

Builds a synthetic variant MKB plus a DK that walks the single
subset-difference slot it carries. `kmp15` is the value of the low byte
of `Kmp[15]` that the chain will land on — pick `0x02` to exercise the
SoftCorrection bit, `0x04` to exercise OnlineChallenge, `0x00`
otherwise.

The fixture pins:
- MKB subset-difference: `u_mask_shift=3, uv=2`. With these masks the
  discriminator bit (u_mask=1, v_mask=0) is bit 2.
- one DK at `node=4, uv=2, u_mask_shift=3`. node 4 has bit 2 set
  (differs from uv=2 on bit 2 → disagrees on v_mask) while agreeing
  with uv on bits 3+ (the u_mask=1 region). dk.uv == MKB.uv and
  dk.u_mask_shift == MKB.u_mask_shift make `dev_key_v_mask == v_mask`,
  so `calc_pk_from_dk` loops zero times — Kp = aesg3(dk, 1).
- one cvalue in record 0x07 chosen so AES-D(Kp, C) ⊕ uv produces a Kmp
  whose byte-15 is exactly `kmp15`.
- record 0x2d (Encrypted Media Key Variant Data): a 32-byte body
  carrying C in the head 16 bytes and a 16-byte Nonce in the tail.
- record 0x2f (Variant Key Data): one 16-byte entry.

Returns `(records, dk, planted_kp, planted_kmp)`.

## Test fixture: `plant_variant_mkb`

Builds a variant MKB by inverting the 2.1 chain for a CHOSEN `(Kp,
Km)`. One subset-difference slot (`uv = 2`, `u_mask_shift = 3`, slot
index 0). The VKD the chain must land on is planted at index **1** of
the `0x2f` table, behind a decoy at index 0, so `VARIANTS[0]` is
load-bearing: it is chosen as `Kvn XOR 1`, and any other value selects
the decoy (wrong `Km`, rejected by the verify gate) or indexes past the
table.

## Test: `variant_chain_derives_the_planted_media_key_for_a_covering_kp`

THE happy path: a Processing Key covering slot 0 of a complete variant
MKB must derive the planted Media Key. This is the assertion the whole
2.1 chain hangs from — `derive_media_key_variant` is what `resolve`
calls for a 2.1 disc, and its output becomes the VUK, the title keys
and every decrypted byte. The assertion lands on the FINAL derived
Media Key, so no intermediate step (VARIANTS lookup, VKD index, Kpnew,
the unwrap) can be replaced by a constant and still pass.

## Test: `mkb_find_mk_dv_returns_the_verify_records_actual_bytes`

`mkb_find_mk_dv` supplies the block the terminal verify gate compares
against. A body answering a FIXED block would make the gate compare
every derived Media Key against a record no disc carries: on a real
disc every correct key is rejected (2.1 discs stop resolving entirely),
and any key that happened to open the fixed block would be accepted
wholesale.

## Test: `variants_for_uv_reads_the_planted_table_entry_that_selects_the_vkd`

`variants_for_uv` reads `VARIANTS[slot]` — the value XORed with `Kvn` to
index the VKD table. A body answering a constant picks the WRONG VKD
entry for every disc, so the derived Media Key fails the verify gate
and every 2.1 variant disc reports `ProcessingKeyUnavailable` with a
perfectly good Processing Key in hand. Asserted two ways: the exact
planted table entry, and — the load-bearing one — that this entry is
what carries the chain to the planted Media Key.

## Test: `variant_uv_slots_drops_zero_uv_and_out_of_range_shift_slots`

`variant_uv_slots` enumerates the slots the chain will try a Processing
Key against, and it must drop the two shapes that are unusable — and
dangerous — rather than pass them on:

- `uv == 0`: no subset-difference. It would be XORed into `Kmp` and `Km`
  as a no-op and the slot would be tried against every VKD entry.
- `u_mask_shift >= 32`: out of range for a `u32` shift. `0x20..=0x3F`
  have the `0xC0` revoked-marker bits CLEAR, so they pass the table
  terminator and reach the `wrapping_shl` in the walk, where shift 32
  silently means shift 0 (`u_mask = 0xFFFF_FFFF`) and matches a slot the
  device does not cover.

Both bytes are disc-supplied. Every existing fixture uses one in-range
non-zero slot, so neither rejection was executed.

## Test: `media_key_variant_from_kp_derives_the_planted_media_key_and_volume_unique_key`

THE happy path for the EXPLICIT-INPUT entry point. `media_key_variant_from_kp`
is the harness twin of `derive_media_key_variant`: same chain, but the
caller supplies the `0x0c` C block, the slot's `uv` and its
`VARIANTS[uv]` instead of having them looked up on the MKB.

Before this test, the ONLY test that entered this function asserted the
`Kmp[15]` soft-correction bit — it returned before the Kpnew, Kvn, VKD,
Km and Kvu steps ever ran. Every arithmetic step past that early return
was executed by nothing, so a body that computed `Kpnew = Kmp | KCD`,
indexed the VKD table at `Kvn + VARIANTS` or dropped the `uv` XOR out of
`Km` produced exactly the same observable behaviour.

The assertion lands on the returned `(Km, Kvu)` — the two values that
become every title key and every decrypted byte on a 2.1 disc.

## Test: `media_key_variant_from_kp_refuses_every_single_wrong_explicit_input`

The terminal gate on the explicit-input entry. `media_key_variant_from_kp`
takes three caller-supplied values (`c_block`, `uv`, `variants_uv`);
each one wrong must yield `MediaKeyVerifyFailed`, never a key. Without
this, a harness feeding a mis-transcribed slot would be handed 16 bytes
that look exactly like a Media Key.

## Test: `plant_walk_variant_mkb`

Builds a two-slot variant MKB keyed by a DEVICE key at slot **1**.
Positions follow the same reasoning as the classical
`derive::position_recovery_tests::plant_mkb`: `uv = 0x0400`
(`u_mask_shift = 12`) with a device node of `0x0C00` satisfies the `[C]`
§3.2.4 gate — equal under `u_mask = 0xFFFF_F000`, different under
`v_mask = 0xFFFF_F800`. The device key's own `uv` equals the slot's, so
`dev_key_v_mask == v_mask` and `calc_pk_from_dk` descends zero levels:
`Kp = AES-G3(dk, 1)`, written out explicitly rather than taken from the
walk's own output.

Slot 0 is a decoy at `uv = 0x0800`, which the SAME device node fails
the `v_mask` half of the gate against (`0x0C00 & 0xFFFF_F000 == 0x0800
& 0xFFFF_F000`), so the walk must skip it and land on slot 1.

## Test: `walk_processing_key_returns_the_covering_slots_key_cvalue_and_index`

`walk_processing_key` must return the Processing Key, `uv`, cvalue AND
slot index of the covering slot — slot **1**, not slot 0. This is the
DK → Kp step the entire 2.1 chain starts from. Every prior test of it
asserted either `None` or merely `is_some()`, and all used a one-slot
MKB where every stride multiplies by zero. A body that read the
subset-difference at the wrong stride, sliced the wrong cvalue block,
or returned the slot-0 cvalue for a slot-1 match would have passed all
of them — and produced a Processing Key that opens nothing.

The expected `Kp` is written as the explicit `AES-G3(dk, 1)`
zero-descent relation from `[C]` §3.2.4, not taken from the walk's own
output.

## Test: `walk_processing_key_refuses_a_device_key_that_fails_the_subset_difference_gate`

The gate the walk applies is `[C]` §3.2.4's subset-difference test, and
a device key that fails it must get NO match. Pinned across all four
coordinates the gate reads — node, uv, u_mask_shift and the key bytes —
because a body that dropped any half of the gate would hand back a
Processing Key derived at the wrong tree position.

## Test: `a_trailing_partial_subset_difference_chunk_is_not_parsed_as_a_slot`

A `0x04` subset-difference record whose byte count is not a multiple of
5 must have its trailing partial chunk REFUSED, not parsed as a slot.

The walk sizes the table with `take_while(|c| c.len() == 5 && ...)`.
Drop the length half of that conjunction and the partial chunk is
counted, and the very next line reads `p_uv[0..4]` off a slice with
fewer than four bytes left — an index-out-of-bounds PANIC on a
disc-supplied record length. This is untrusted input: a truncated or
crafted MKB reaches this with no other guard in between.

## Test: `a_cvalue_table_shorter_than_the_matching_slot_is_not_sliced_past`

A `0x0c` cvalue table SHORTER than the matching slot index must make
the walk skip the slot, not slice past the end of the record.
`cvalues[uvs_idx * 16..(uvs_idx + 1) * 16]` is an unchecked slice; the
only thing in front of it is `if uvs_idx >= cvalues.len() / 16`. The two
counts come from DIFFERENT disc-supplied records (`0x04` and `0x0c`),
so nothing but this guard keeps them in agreement — a real MKB with a
short cvalue table panics the rip thread without it.

## Test: `walk_processing_key_needs_either_the_verify_magic_or_variant_records`

The classical-magic escape hatch. On a NON-variant MKB the walk must
return a match only when `AES-D(Kmp, mk_dv)` opens with the `[C]`
§3.2.5.1.4 verify magic; on a variant MKB that relation does not hold
(the walk yields a Precursor) and the presence of `0x2d`/`0x2f` is what
lets the match through to the chain's own terminal gate.

Both halves of `classical_ok || variant_present` are pinned here: strip
the variant records from a fixture whose magic does NOT hold and the
walk must go quiet. Otherwise a body that dropped the guard entirely
would return an unauthenticated Processing Key on every classical MKB.

## Test: `walk_processing_key_authenticates_a_classical_match_through_the_verify_magic`

The OTHER half of `classical_ok || variant_present`: a non-variant MKB
whose cvalue really does open the Verify-Media-Key magic must yield a
match, and the `[C]` §3.2.4 relation that produces the candidate —
AES-D(Kp, cvalue) with `uv` XORed into the LOW FOUR BYTES — must be
computed exactly.

This is the only path on which that XOR is observable. On a variant MKB
`variant_present` short-circuits the magic test, so the whole
`km_candidate` computation is dead weight there: a body that ORed `uv`
in, or XORed it at the wrong offset, changes nothing any variant
fixture can see. On a CLASSICAL MKB it is the entire authentication of
the Processing Key.
