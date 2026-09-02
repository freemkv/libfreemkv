# `src/aacs/resolve.rs` — internal rationale

Overflow detail relocated here by the comment-guard pass; each section is
pointed to by a short `//` comment at the named site in `resolve.rs`.

## `resolve_keys_classical`

Resolve all AACS keys for a disc using the classical (single-stage Media Key
derivation) paths. Used by both V10 and V20.

Paths run in root-of-trust -> per-disc-leaf order. A match at any path
returns immediately:

1. MKB + device keys -> processing key -> media key -> VUK
2. MKB + processing keys -> media key -> VUK
3. KEYDB MK + matching VID -> derived VUK
4. KEYDB disc-hash -> VUK
5. KEYDB disc-hash -> pre-decrypted unit keys (no VUK)

## `unique_verifying_mk`

The MK-pool selection rule of path 2.5, split out of `resolve_keys_v1` so the
ambiguity guard has a reachable test.

`verifies` is the MKB check -- in production
`AES-D(mk, mk_dv)[..8] == MK_VERIFY_MAGIC`. Returns a Media Key only when
EXACTLY ONE DISTINCT candidate passes. Duplicates of the same key are one
candidate (a pool aggregated across providers routinely repeats a key), but
two DIFFERENT keys that both verify mean the pool cannot say which is this
disc's Km: picking either derives a wrong VUK, and a wrong VUK decrypts to
plausible-looking garbage rather than failing loudly. Bail and let the later
hash/VID paths answer instead.

The predicate is a parameter rather than the inlined AES check because a
genuine two-key multi-hit cannot be synthesised: it needs one ciphertext
that decrypts under two distinct AES-128 keys to plaintexts sharing a 64-bit
prefix -- a 2^64 search. Injecting the verifier is the only way the
ambiguity branch is reachable from a test at all.

## Test: `mk_pool_ambiguity_bails_rather_than_picking_a_media_key`

Path 2.5's ambiguity guard: when MORE THAN ONE DISTINCT pooled Media Key
verifies against the MKB, the resolver must return no key at all rather than
pick one. A wrong Km derives a wrong VUK, and a wrong VUK does not fail
loudly -- it decrypts the title to garbage that muxes and plays as a corrupt
rip.

The real MKB check cannot be forced into a multi-hit: two distinct AES-128
keys decrypting one `mk_dv` to plaintexts that share the 64-bit verify magic
is a 2^64 search, not a fixture. So the rule is tested through
`unique_verifying_mk`, whose verifier is a parameter -- the same function
`resolve_keys_v1` calls, with the same pool semantics.

## Test: `resolve_keys_v21_treats_the_all_zero_volume_id_as_no_vid`

`resolve_keys_v21` gates paths 1 and 3 on `has_vid`, and an all-zero Volume
ID is the crate's "the VID was never read" sentinel -- the SCSI handshake
leaves the buffer zeroed when it does not run or fails.

Both directions matter and both fail silently:

- treating the zero sentinel as a real VID runs path 3 and derives
  `Kvu = AES-G(Km, 0...0)`, a perfectly well-formed but WRONG VUK. It
  unwraps the title keys to garbage, and nothing downstream errors -- the
  rip just decodes to noise.
- treating a real VID as absent skips paths 1 and 3 entirely, so a disc
  that could have been resolved from its Media Key reports no key.

Asserted through the final VUK, not through the flag.

## Test: `classify_vid_present_with_material_is_no_material_not_vid`

A NON-zero VID present, but resolution still fails (the providers carry
material that doesn't resolve this disc). The VID is available, so the
failure is NOT "VID unavailable" -- it must classify NoMaterial regardless
of how much derivation material is present, because re-acquiring the VID is
not the fix. This is the `has_vid == true` short-circuit, which no existing
test covers (the gate test only uses the zero-VID sentinel).
