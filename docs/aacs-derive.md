# `src/aacs/derive.rs` — key derivation notes

Rationale and background relocated here from doc comments in `derive.rs` to
keep those comments within the repo's comment-length limits.

## `derive_media_key_from_pk` — why the direct PK × cvalue scan

A Processing Key is **terminal**: it is the key at its Subset-Difference
node, one `AES-G` from the Media Key. So the fast path tries each PK
*directly* against the MKB cvalue tables (no tree descent). On a large
AACS 2.x UHD MKB (~181k cvalues) this is ~15x faster than treating a PK as
a device-node label and walking the tree.

MKB record types relevant to this path:

| Type | Meaning |
|------|---------|
| `0x10` | Type and Version Record (has MKB version) |
| `0x81` | Verify Media Key Record, AACS 1.0 (has `mk_dv`) |
| `0x86` | Verify Media Key Record, AACS 2.0/2.1 (has `mk_dv`) |
| `0x04` | Subset-Difference Index (has UVS entries) |
| `0x05` | Media Key Data Record (cvalues, 1:1 with `0x04`) |
| `0x07` | Explicit Subset-Difference Record (NOT cvalues) |

## `recover_dk_position` — search strategy and cost

This finds a device key's tree node empirically: for each MKB
subset-difference record, it tries the device at the record's node AND at
every ancestor v-position (the device may sit one or more levels ABOVE the
record, descending via AES-G3 to reach it), deriving the candidate
Processing Key DIRECTLY (one `calc_pk_from_dk` per candidate, no full
re-walk) and checking it validates against that record's cvalue.

Cost is `O(slots × tree_depth)` — linear in the MKB's subset-difference
index, not the quartic cost of re-deriving per candidate. Slots are
independent so the scan parallelizes (~181k slots, ~26s serial on a UHD
MKB); `find_map_any` returns the first match and cancels the rest.

## `resolve_candidate` — composed primitives and `None` conditions

Composes the raw derivation primitives (`derive_media_key_from_pk`,
`derive_media_key_and_pk_from_dk`, `derive_vuk`, `derive_unit_keys`) and
parses `Unit_Key_RO.inf` at the version the disc's MKB declares, so a
multi-CPS disc yields all its unit keys from the one candidate.

Validate `unit_keys` against a real encrypted unit with `decrypt_unit` +
`is_clean_ts` to prove the candidate actually opens the disc — this function
is pure derivation, no sampling or validation.

Returns `None` only when derivation itself cannot proceed: a PK its MKB
rejects, a `Dk` the MKB can't process, a missing VID on a path that needs
one, or an unparseable/empty `Unit_Key_RO.inf`.

## Test fixtures — inverting the AACS relations

There are no published AACS test vectors, but none are needed: the AACS
relations (`[C]` §3.2.3–§3.2.5) are invertible, so a valid MKB for a CHOSEN
key can be constructed with `aes_ecb_encrypt` and the same `aesg3` the walk
uses as its node function. `plant_mkb` and its siblings in
`position_recovery_tests` do exactly this — no real key material, and the
assertions check the DERIVED Media Key, not any intermediate the code under
test also produces.

`plant_mkb`'s single-slot fixture picks `uv = 0x0400` (lowest set bit 10)
with `u_mask_shift = 12` so a gating device node exists: `resolve_dk_node`
flips one bit `b < 12`, and the walk's gate (`[C]` §3.2.4) needs that bit
inside `v_mask` (`0xFFFF_FFFF << 11`) and outside `u_mask`
(`0xFFFF_FFFF << 12`) — i.e. `b == 11`.
