# AACS Crypto Primitives — Implementation Notes

Rationale and background for the internal (non-public) primitives in
`src/aacs/crypto.rs`. See [AACS Encryption](aacs.md) for the public-facing
key resolution and decryption story.

## `aes_cbc_encrypt`

Forward direction of `aes_cbc_decrypt`, and its exact inverse (`[C]` §2.1.2,
`AES-128CBCE`). `data.len()` must be a multiple of 16; the `debug_assert!`
documents/enforces that contract.

Constructs the cipher ONCE for the whole slice. Driving this from the
single-block `aes_ecb_encrypt` instead rebuilds the AES key schedule per
16-byte block, which for a 6144-byte aligned unit is 383 redundant key
expansions.

## `aes_cbc_decrypt`

AES-128-CBC decrypt in-place with the fixed AACS IV (`[C]` §2.1.2,
`AES-128CBCD`). `data.len()` must be a multiple of 16; any trailing partial
block is silently ignored. All callers pass aligned regions (6128 and 2032
bytes), and the `debug_assert!` documents/enforces that contract.

This doc was once orphaned onto `aes_cbc_encrypt` above it, when that
function was inserted directly after it with no separating blank line:
rustdoc rendered the crate's only forward-direction AACS primitive as
"decrypt" and cited the spec's DECRYPT clause for it, while this function
had no doc at all. `encrypt_unit_is_the_exact_inverse_of_decrypt_unit` in
`content.rs` pins the directions behaviourally, so a maintainer "fixing" the
contradiction by swapping the two bodies fails the suite instead of shipping
a second decryptor behind an already-set encrypted flag.

## `cbc_decrypt_blocks`

Split out of `aes_cbc_decrypt` so a caller that decrypts several regions
under one loop-invariant key expands the schedule once. `decrypt_bus`
(`super::content::decrypt_bus`) is that caller: bus encryption (`[C]` §4.2 /
the AACS 2.0 Read Data Key) covers bytes 16..2048 of EVERY 2048-byte
sector, so a 6144-byte aligned unit is three regions under one
`read_data_key` — three key schedules where one suffices, on the per-unit
decrypt hot path of a whole 90 GB read.

## `aes_g`

AES-G(x1, x2) = AES-128D(x1, x2) XOR x2 (`[C]` §2.1.3, note: uses
AES-128**D**). The Media Key Variant chain uses AES-G to derive both the
variant number (`Kvn = AES-G(Kp, Nonce)`) and the Volume Unique Key
(`Kvu = AES-G(Km, VID)`). See `super::derive::derive_vuk` for the classical
VUK form — the math is identical, this exposes it as a neutral primitive
for the variant chain.

## `aesg3`

AACS-G3 (`[C]` §3.2.2, Triple AES Generator): `left = D(k,s0)⊕s0` inc 0,
`pk = D(k,s0+1)⊕(s0+1)` inc 1, `right = D(k,s0+2)⊕(s0+2)` inc 2. Shared
with `super::variant` (its variant chain runs the same SD tree); a single
definition keeps the two walks byte-identical.

## Test: `S0`

The AACS-G3 seed `s0`, transcribed independently from `[C]` §3.2.2 rather
than read from `AESG3_SEED` — a test that sourced the seed from the
production constant would assert that constant against itself and would
still pass if it were edited.

## Test: `aesg3_inverts_to_the_spec_seed_under_aes_encrypt`

`aesg3` is the node function of the AACS subset-difference tree: every
Processing Key the DK walk produces (`aesg3(node_key, 1)`) and every descent
step (`aesg3(., 0)` / `aesg3(., 2)`) is one call. A body that returned a
fixed block would make every device key in the crate derive the SAME
Processing Key, and a `^` that became `|` or `&` would derive a
wrong-but-plausible one — in both cases the MKB walk simply stops finding
Media Keys, with no error to say why.

Pinned through the spec relation rather than a re-implementation: `[C]`
§3.2.2 defines `AES-G3` as `AES-128D(k, s) XOR s` for `s = s0 + inc` (added
into the last seed byte), so applying the FORWARD primitive
`aes_ecb_encrypt` — a different function from the one under test — to
`aesg3(k, inc) XOR s` must reproduce `s` exactly.

## Test: `aesg3_yields_three_distinct_subkeys_for_the_three_increments`

The Triple Generator's three outputs (`[C]` §3.2.2: left = inc 0, the
Processing Key = inc 1, right = inc 2) are the two child node keys and the
Processing Key of ONE tree node. They must be three different keys — if
`inc` were ignored, a descent would revisit its own parent and the walk
would derive the same key at every level of the tree.
