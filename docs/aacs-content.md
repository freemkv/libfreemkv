# AACS Content Decryption — Implementation Notes

Rationale and background trimmed out of `src/aacs/content.rs` doc comments to
satisfy `ci/comment-guard.py`'s per-audience prose caps. See
[AACS Encryption](aacs.md) for the public-facing key resolution story and
[AACS Crypto Primitives](aacs-crypto.md) for the low-level AES primitives.

## `is_unit_aligned`

AACS aligned units (6144 B / 3 sectors) are anchored at the start of each
clip's encrypted region, so a read must begin a whole number of units past
that base for `decrypt_sectors` (which anchors units at buffer offset 0) to
align the CBC correctly. This is the single source of truth for the
decrypt-on-read gate, the inline and highway mux read paths, and the
key-validation sample reader — all key off this, never absolute `lba % 3`. A
disc whose clip `start_lba` is not itself 3-aligned would otherwise mis-gate
(reject readable units, then report "Decryption failed") on exactly the
titles whose clips land off a 3-boundary.

`saturating_sub` makes the `lba < unit_base` case well-defined — it clamps
the offset to 0, a unit boundary — rather than the latent `wrapping_sub` trap
where an underflow wraps to ~2^32 and, because `2^32 ≡ 1 (mod 3)`,
mis-reports the alignment (e.g. `lba == unit_base - 1` would falsely read as
aligned).

## `aacs_unit_encrypted`

The mux is the principal consumer of this signal for `BdTs` discs (via
[`is_clean`], see below). It is readable WITHOUT a key. CRITICAL: only
meaningful when `unit` is read at the correct clip-FILE-anchored boundary — a
disc-absolute / mis-aligned read makes the flag byte arbitrary mid-stream
data, which is why per-unit checks run clip-anchored, not in the whole-disc
sweep.

## `aacs_unit_needs_decrypt`

Buffer-iterating sites that may run twice over the same `buf` (the post-fetch
re-decrypt, sample collection, failure diagnosis) need an IDEMPOTENT "does
this still need work?" test, so `aacs_unit_encrypted` (the authoritative flag,
which decryption never rewrites) is composed with a "structure restored?"
check that flips once decrypted: TS syncs come back for `BdTs`; valid
`00 00 01 BA` packs come back for `MpegPs`.

## HD-DVD scramble flag (`PS_SCRAMBLE_OFF` / `PS_SCRAMBLE_MASK`)

BD/UHD/FMTS flag encryption uses the Copy Permission Indicator in the top 2
bits of byte 0 (the M2TS `TP_extra_header`). HD-DVD `.evo` is Program Stream —
byte 0 is a `00 00 01 BA` pack_start_code, NOT a CPI — so AACS reuses the
MPEG-2 `PES_scrambling_control` field instead: pack_header (14 bytes) + PES
start-code/`stream_id` (4) + `PES_packet_length` (2) puts the PES flags byte at
offset 20, with `PES_scrambling_control` in bits 5-4 (`& 0x30`). Non-zero =
encrypted. Derived from BackupHDDVD (`Header[20] & 0x30`) cross-checked
against MPEG-2 systems (ISO/IEC 13818-1).

UNVERIFIED against a real ENCRYPTED HD-DVD disc — none available to confirm
byte-exactly. Two open questions a real disc must settle:
1. A pack carrying an MPEG `system_header` (`00 00 01 BB`) before the PES
   packet shifts this offset past 20.
2. Whether offset 20 is even readable pre-decrypt. BD keeps only the first 16
   bytes clear (the seed) and AES-CBC-encrypts 16..6144 — under that model
   offset 20 is ciphertext. BackupHDDVD reading `Header[20]` pre-decrypt
   implies HD-DVD instead encrypts PES *payloads* with *clear* pack/PES
   headers (per-PES model). If so, `decrypt_unit`'s 16-byte-seed model also
   would not fit HD-DVD and needs its own path. TS is unaffected either way.

## `KEY_PROOF_PACKETS`

Four `0x47` syncs are 32 bits of MPEG-TS structure, but the per-UNIT
false-pass risk is NOT 2^-32: `is_clean_ts` accepts ANY four of the ~31
encrypted packets in a 6144-byte aligned unit, so for a wrong key (uniform
AES noise, `0x47` at 1/256 per packet) it is ≈ C(31,4)·256^-4 ≈ 7e-6, i.e.
~1e-5. 1-in-4-billion is the probability for four SPECIFIC packets and
overstates the margin by ~4000x; at `KEY_PROOF_PACKETS = 3` the per-unit rate
is ≈ C(31,3)·256^-3 ≈ 2.6e-4, so do NOT lower it on the strength of slack
that is not there. It is an ABSOLUTE proof floor, NOT a proportion — a unit
the key opened but whose content is bad-encoded (many non-conforming
packets) is proven by ANY four good packets, not rejected for the bad ones.

## `is_clean_ts`

The mux is its principal consumer for `BdTs` discs, reaching it through
`is_clean`: `mux::resolve`'s multi-CPS `pick` closure selects a unit key by
it, `probe_index_phase` reports each FMTS index's interleave parity by it,
and `decrypt::decrypt_sectors_mapped` uses it as the forensic-range verify
net. (The doc used to say "the mux never calls this", which invited a
maintainer to tighten or loosen the proof rule believing only whole-disc read
verification was affected — while it in fact changes which unit key a
multi-CPS disc muxes with and which phase an FMTS index is muxed at.)

Rule — evidence is ABSOLUTE, scaled to the packets that exist. Over the
ENCRYPTED packets (skip packet 0: its `0x47` sits in the clear 16-byte seed,
so it reads `0x47` for ANY key and is never evidence), let `E` = non-padding
content packets and `synced` = those carrying `0x47`. The key opened the unit
iff `E == 0` (nothing encrypted to prove) OR `synced >= min(E, KEY_PROOF_PACKETS)`.
* WRONG key → ~0 synced → fails (reaching 4 by chance ≈ 1e-5/unit, and every
  unit of a clip would have to fluke — astronomically safe).
* RIGHT key, bad-encoded content → any 4 good packets pass; the bad ones are
  the muxer's problem. (This is the false-negative the old 75% PROPORTION
  caused — a mostly-bad unit the key opened was wrongly rejected.)
* `min(E, 4)` handles the end-of-clip fragment TAIL: a unit with only E=1
  real packet (then source-zero padding) needs just that one to sync, so a
  sparse-but-valid tail is never false-rejected. Padding (all-zero payload)
  is excluded throughout.

## `is_clean_ps`

The Program-Stream arm of `is_clean` (HD-DVD `.evo`): every 2048-byte pack
begins with the pack_start_code `00 00 01 BA`; a 6144-byte AACS unit spans
three packs. Pack 0's start sits in the clear 16-byte seed (present
regardless of the key — mirrors `is_clean_ts` skipping packet 0); packs 1
and 2 (offsets 2048 and 4096) are in the encrypted region, so a wrong key
garbles them and this returns false (64 bits of discrimination). Validated
against real decrypted HD-DVD `.evo` (two real retail titles): pack starts
are exactly 2048-aligned, three per aligned unit, at offsets 0 / 2048 / 4096.

UNVERIFIED-HDDVD-DECRYPT: the pack structure here is confirmed on decrypted
rips, but that a real ENCRYPTED `.evo` decrypts to it via the same
6144-byte aligned unit (16-byte seed + AES-CBC over 16..6144) as BD is the
unverified assumption — we have no encrypted HD DVD. If HD-DVD decryption
yields garbage with a known-good key, the unit granularity is the suspect.

## `decrypt_unit` / `encrypt_unit`

Block Key = AES-128E(Kcu, seed) ⊕ seed (`[BD]` §3.10.1 Fig 3-8: encrypt the
clear 16-byte seed under the CPS Unit Key, XOR the seed back in — the
trailing ⊕ seed is load-bearing); then AES-128-CBC decrypt/encrypt bytes
16..6144 under the AACS IV. `encrypt_unit` is the exact inverse of
`decrypt_unit`, for authoring an encrypted disc image (and for building
genuinely-encrypted read-path fixtures); it does NOT set the encrypted flag,
because where that flag lives is container-specific, and keeping it out
keeps the crypto container-agnostic.

Source-zero padding packets (all 192 bytes zero on disc) are restored to
zero by `decrypt_unit`: their decrypted bytes are AES-noise from decrypting
zeros, but the source WAS zero, so writing the true source back is faithful
and gives the demux a tidy gap. Content packets are left EXACTLY as
decrypted — the decrypt path never rewrites content, so an authored-bad
packet passes through verbatim. `encrypt_unit` deliberately does NOT mirror
this: an all-zero plaintext packet encrypts to ciphertext that is not
all-zero, so it is not mistaken for padding on the way back and the round
trip is still exact. Authoring that wants true source-zero padding leaves
those packets unencrypted instead.

`encrypt_unit` returns `false` — encrypting nothing — when `unit` is shorter
than `ALIGNED_UNIT_LEN`. That case MUST be checked: the caller has already
set the container's encrypted flag by then (the contract requires
flag-before-crypto, since the header is the key seed), so ignoring the
result leaves a unit marked encrypted while still carrying plaintext, which
is the worst possible outcome for an authoring tool.

## `decrypt_bus` key-schedule regression (test: `decrypt_bus_expands_the_read_data_key_once_per_unit`)

MEASURED, not reasoned: `decrypt_bus` used to call `aes_cbc_decrypt` once per
2048-byte sector, and each call built its own AES-128 key schedule, so a
6144-byte aligned unit performed THREE key expansions under the same
loop-invariant `read_data_key`. On a 90 GB UHD read on a stock
(non-firmware-unlocked) drive — ~14.6 million aligned units — that is ~29
million redundant expansions on the per-unit decrypt hot path, for a key
that is constant for the whole disc. The counter is incremented inside
`crypto::new_cipher`, the single construction site.

## `IV0_PUBLISHED` (test fixture)

`[C]` §2.1.2 fixes one default CBC IV for every AACS AES-CBC operation. Both
IV tests used to compute their expected value from `crypto::AACS_IV` itself,
so the constant was asserted against itself and NOTHING in the suite pinned
its bytes: swapping `AACS_IV` for `[0u8; 16]` left both tests passing (one
builds its ciphertext with the same value and the other cancels the change
in a triple XOR) while every real AACS disc decrypted to noise — block 0 of
every 6144-byte aligned unit and of every bus-encrypted sector XORed with the
wrong IV. `IV0_PUBLISHED` is the independent witness the tests assert
against instead.

## Encrypted-flag readers (`aacs_unit_seed_encrypted`, `aacs_unit_encrypted`)

`aacs_unit_seed_encrypted` is the flag reader for a PARTIAL unit — the guard
that stops a truncated encrypted fragment from being emitted as clear
content. It reads ONLY the two Copy Permission Indicator bits (`[BD]`
§3.10.2, byte 0 bits 6-7); the remaining six bits are `TP_extra_header`
arrival-timestamp bits and carry no encryption meaning. Both failure
directions are damaging and silent: a reader that answers "encrypted" for a
clear fragment discards good content, and one that answers "clear" for an
encrypted fragment writes ciphertext into the output as if it were video.

`aacs_unit_encrypted` is the AUTHORITATIVE gate and requires a WHOLE
6144-byte aligned unit: on anything shorter the flag byte is not guaranteed
to be the unit's, so it must answer `false` and leave the partial-unit case
to `aacs_unit_seed_encrypted`. A reversed length guard would both classify
fragments off arbitrary mid-stream bytes and, on an empty slice, index out
of bounds.
