# CSS keyless title-key recovery (`src/css/keyless.rs`)

## The cipher this attacks

The content descrambler ([`super::lfsr::descramble_sector`]) seeds its two
LFSRs **directly** from `key = title_key XOR sector_seed` (seed =
`sector[0x54..0x59]`): LFSR1 from key/seed bytes 0-1, LFSR0 (24-bit) from
bytes 2-4 with the pre-conditioning `r0 = r0*2 + 8 - (r0 & 7)`, and each body
byte recovered as `plain = TAB1[cipher] ^ (keystream & 0xff)`. There is no
title-key mangling on the content path, so the recovery is a single inversion
of the sector cipher.

## The attack

1. **Known plaintext → keystream.** Because descramble applies TAB1 to the
   ciphertext, the per-byte keystream is `TAB1[cipher[i]] ^ plain[i]`.
2. **Brute the 16-bit LFSR1 seed.** For each of 2^16 seeds, run LFSR1
   forward; for the first four steps deduce the LFSR0 output bytes from the
   keystream (carry-tracked), reconstructing LFSR0's state. For the next six
   steps clock LFSR0 normally and check it reproduces the keystream — a wrong
   LFSR1 seed fails fast.
3. **Back-clock LFSR0.** Run four backward steps (each a 256-way search for
   the byte shifted in) to reach the initial state, then undo the
   `r0*2 + 8 - (r0 & 7)` pre-conditioning to recover key[2..5].
4. **XOR back the seed.** `key[0..5] ^= sector_seed[0..5]`.

Known plaintext for step 1 comes from the longest periodic run in the
cleartext `sec[0x00..0x80]`, assumed to continue into the encrypted region at
0x80.

## `recover_title_key_from_plain`

Core of the recovery. `crypted` is the ciphertext starting at sector byte
0x80; `decrypted` is the matching known plaintext; `seed` is
`sector[0x54..0x59]`. On success returns the recovered 5-byte title key;
`None` if no LFSR seed reproduces the keystream. At least 10 bytes of
`crypted`/`decrypted` are required (the cipher is iterated 10 times: 4 to
reconstruct LFSR0, 6 to validate).

## `attack_crib`

Scans the clear header `sec[0x00..0x80]` (never scrambled) for the longest
run that repeats with a cycle length in 2..0x2F. If the run is long enough
(`plen > 3` and at least two full cycles), the plaintext at 0x80 is taken to
be that periodic run continuing forward. Returns `None` for an unscrambled
sector or one with no usable run — such a sector can be neither cracked nor
key-validated, only descrambled with an externally-cached key.

The header is untouched by `descramble_sector`, so the crib is identical
before and after descramble: the decrypt path uses it as a per-sector
"did the cached key descramble correctly?" oracle (the predicted plaintext
must reappear at 0x80), and the cracker uses it as its known plaintext.

## Tests

### `synth_sector`

Builds a synthetic scrambled sector for a given title key and seed, with
`plain` placed as the plaintext at byte 0x80, scrambled with EXACTLY the
cipher `descramble_sector` inverts. Returns
`(scrambled_sector, full_plaintext_body)`.

### `synth_periodic_sector`

Builds a synthetic scrambled sector whose CLEARTEXT (0x00..0x80) ends in a
periodic run that continues into the encrypted region — the case
`crack_title_key` is designed to crack.

### `recover_round_trips_known_keys`

MANDATORY round-trip (Task C.1): synthesize a scrambled sector for a known
(title_key, seed), then assert `recover_title_key` returns a key that
descrambles the body back to plaintext. CSS title-key recovery is
well-defined up to keys that scramble identically; we assert the full body
round-trips (the true correctness property), and additionally that the EXACT
key is returned for the common case.

### `descramble_matches_accepts_only_the_key_the_sector_was_scrambled_with`

`descramble_matches` is the ONLY gate between the LFSR search and a key
handed back to the caller: both `recover_title_key` and the crib-driven
`crack_title_key_inner` return a candidate only if this says the key really
descrambles the sector to the known plaintext. A body that always answered
`true` would let the first spurious LFSR-seed match through as the title
key — the ripper would then descramble the whole title with a key that opens
nothing, producing garbage rather than a "no key" error.

Pinned both directions: the genuine key is accepted, and EVERY key one bit
away from it is rejected. The one-bit neighbours are the strongest form of
wrong key — a gate that only rejects wildly different keys would still pass
a near-miss out of the 2^16 seed search.

### `descramble_matches_does_not_disturb_the_caller_s_sector`

The gate is applied to a COPY: verifying a candidate must not modify the
caller's sector. `recover_title_key` runs the gate and then hands the sector
on to be descrambled for real — if verification descrambled in place, that
second descramble would run over already-transformed bytes (and, worse, a
rejected candidate would leave the sector corrupted).

### `recover_rejects_a_sector_that_ends_inside_the_encrypted_region`

A sector buffer that ENDS inside the encrypted region must be refused, not
sliced. `recover_title_key` slices `sector[0x80..0x8A]` unconditionally
after its length guard. The existing short-sector test uses
`SECTOR_BYTES - 1`, which is still long enough for that slice to succeed —
so the guard was never the thing producing the `None`, and dropping it (or
weakening the `||` to `&&`, which a full-length crib satisfies) changed
nothing observable. On a real short read this is an out-of-bounds panic on
the rip thread.

### `a_buffer_longer_than_one_sector_still_yields_its_key`

A buffer LONGER than one sector is still one sector: both entry points read
the first `SECTOR_BYTES` and must recover the key from it. Callers read DVD
data in multi-sector blocks, so an over-long slice is the normal case, not
an exotic one. A length guard that rejected it (`len > SECTOR_BYTES` instead
of `<`) would make every block-read caller silently unable to crack
anything.

### `recover_accepts_more_than_ten_bytes_of_known_plaintext`

`recover_title_key` accepts MORE than ten bytes of known plaintext, and uses
all of it: the extra bytes tighten the `descramble_matches` gate. The
ten-byte figure is a MINIMUM (the cipher is iterated ten times), not an
exact requirement — a guard reading it as an upper bound would reject every
caller that knows a longer crib.

### `a_recoverable_sector_with_the_scramble_bits_cleared_is_still_refused`

The scramble-flag gate on a sector whose BODY really is ciphertext. Both
entry points refuse a sector with `sector[0x14] & 0x30 == 0`: an
unscrambled sector has no title key to recover, and its bytes at 0x80 are
already plaintext. Every prior test of this gate used an all-zero or
all-`0x11` sector, where the recovery would have found nothing anyway — so
widening the mask test (`&` to `|`, which makes it true for EVERY flag byte)
produced the same `None` and went unseen.

Here the sector is genuinely scrambled and its key IS recoverable; only the
cleared flag stands in the way. If the gate stops working, both functions
start returning keys for sectors the disc says are in the clear.

`attack_crib` carries its own copy of the same gate and is the one that
actually stops the crack (`crack_title_key`'s is defensive duplication); it
also doubles as the decrypt path's cached-key oracle.

### `descramble_matches_forces_the_scramble_flag_on_its_own_copy`

The gate must verify a candidate against the sector's CIPHERTEXT regardless
of what the sector's own flag byte says. `descramble_matches` forces `0x10`
on its copy precisely because `super::lfsr::descramble_sector` is a no-op
when the scramble bits are clear — without that, verifying a
scrambled-but-unflagged sector compares raw ciphertext against the crib, and
every candidate key is rejected. Nothing exercised it: every fixture already
had the flag set, where forcing the bit is a no-op.

### `descramble_matches_compares_all_of_the_plaintext_and_no_more_than_the_sector`

The gate compares the WHOLE supplied plaintext, clamped to the encrypted
region. Two properties in one, because they are the two halves of
`plain.len().min(SECTOR_BYTES - ENCRYPTED_START)`:

- it must compare beyond the first sixteen bytes, or a key that opens only
  the head of the crib is accepted; and
- it must never compare past the end of the sector — a caller that knows
  more plaintext than the 1920-byte encrypted region holds otherwise
  indexes off the end of the buffer and panics.

### `sector_with_trailing_run`

Builds a sector whose clear header ends in a `period`-length repeating run
of exactly `run_len` bytes immediately before 0x80. The run is anchored to
ABSOLUTE sector offset (`sec[x] = pat[x % period]`), which is what makes
"the run continues past 0x80" a statement independent of the code under
test: the byte at `0x80 + i` of the underlying plaintext is
`pat[(0x80 + i) % period]`. Everything before the run is `0x00` (the pattern
bytes are all >= 0xD0, so the run cannot be extended backwards by accident),
and the encrypted region is filled with `0xFF` — so a crib that reads past
0x80 into "ciphertext" is immediately visible.

### `attack_crib_predicts_the_periodic_run_continuing_past_0x80`

KNOWN ANSWER: for a run of `run_len` bytes with period 5 ending exactly at
0x80, the crib is the run continued forward — the same ten bytes for every
run length, because the prediction depends only on the pattern and the
phase, never on how many cycles happened to be visible. The short lengths
are the load-bearing ones: at `run_len = 11` the crib window starts at 0x76
and is only 10 bytes from the end of the header, so any drift in
`plain_start`, in `cycles * best_p`, or in the `i % best_p` wrap reads the
0xFF "ciphertext" instead of the run.

### `attack_crib_refuses_a_run_shorter_than_two_cycles`

A run of exactly ONE cycle (plus the trivial tail the detector counts) is
not enough to predict forward: `attack_crib` requires at least two full
cycles. Weakening that guard would let a one-off byte sequence be declared
periodic and produce a confidently wrong crib — which the decrypt path uses
as its "is my cached key still right?" oracle.

### `attack_crib_refuses_a_buffer_shorter_than_a_sector`

`attack_crib` indexes `sector[0x7f - j]` with no per-access bound, so its
own length guard is the only thing between a short buffer and an
out-of-bounds read. Nothing reached it: every caller-level test used a full
sector, and the entry points' guards fire first.

### `attack_crib_survives_a_header_that_is_periodic_to_offset_zero`

A header that is periodic ALL THE WAY to offset 0 must not walk the
backward scan off the front of the sector. The detector counts backwards
from 0x7f while `j < 0x80`. On a fully periodic header the run never
breaks, so `j` reaches 0x7f and the bound is the ONLY thing that stops it —
one step further and `0x7f - j` underflows a `usize` and panics. A constant
or fully-patterned 128-byte header is ordinary DVD data (padding, a run of
zeros), not a crafted input, and every existing fixture had a
filler/run boundary well before offset 0 that stopped the scan early.

### `attack_crib_is_independent_of_the_encrypted_region`

The crib is read from the CLEAR header only. A run that reaches 0x80 must
predict from the header bytes, never from the encrypted region — the
previously-fixed bug this function's doc comment records. Pinned by
rewriting the encrypted region and requiring the crib not to move.

### `recover_title_key_from_plain_refuses_fewer_than_ten_bytes_of_either_input`

`recover_title_key_from_plain` unconditionally builds a 10-byte keystream
buffer from `crypted[0..10]` and `decrypted[0..10]`, so its length guard is
the only thing standing between a short slice and an index-out-of-bounds
PANIC. Nothing reached that guard before: `recover_title_key` rejects
`plain.len() < 10` at its own door and always hands on exactly ten
ciphertext bytes, and `crack_title_key_inner` always passes a fixed
`[u8; 10]` crib. The guard is a live contract for any future caller and was
executed by no test at either boundary.

### `recover_title_key_from_plain_xors_the_sector_seed_back_out`

The seed XOR-back (`recover_title_key_from_plain`'s last step) is what turns
the recovered LFSR key into the TITLE key: `key ^= sector_seed`. Pinned as a
known answer across seeds that differ only in one byte — the same
ciphertext/plaintext pair therefore must yield title keys differing in
exactly that byte. Without this, a body that ORed the seed in (or dropped
the step) still round-trips on any fixture whose seed is zero, and on the
non-zero ones the failure looks like "no key found" rather than a wrong
step.
