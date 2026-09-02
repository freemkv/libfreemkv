# src/mux/ebml.rs

## `fixed_width_vint8`

Encodes `data_size` as the FIXED-WIDTH 8-octet EBML VINT used to back-patch
a master element's size field: the `0x01` VINT_MARKER octet followed by the
56-bit VINT_DATA payload, big-endian (RFC 8794 section 4.4 — an 8-octet
VINT carries 7 octets of VINT_DATA).

Extracted so the full 56-bit payload can be exercised directly: reaching
the high payload bytes through `end_master` / `end_master_buf` would take
a multi-terabyte buffer, leaving them unconstrained by any test.

`data_size` must be below 2^56; both callers check that first.

## `start_master_buf` / `end_master_buf`

This is the seek-free twin of `start_master`/`end_master` for elements
small enough to assemble whole before hitting the file (a BlockGroup: one
Block plus a couple of tiny elements). Because `end_master` always
back-patches a FIXED-WIDTH 8-byte VINT (`0x01` + 7 payload bytes) rather
than a minimal-width one, the bytes produced by this pair are byte-for-byte
identical to the seek-and-back-patch pair — assembling in memory cannot
change the emitted Matroska.

## `read_exact_bounded`

Reads exactly `len` bytes WITHOUT trusting `len` to size the allocation.

`vec![0u8; len]` on an attacker-controlled EBML size would allocate
gigabytes before the read fails. Instead the reader is capped to `len`
and the buffer grows as bytes actually arrive: a malformed element that
claims a huge length but supplies few bytes allocates only what it
delivers, then errors on the short read.

## Test: `buffered_master_matches_seeking_master_byte_for_byte`

The buffer-based master helpers must produce byte-for-byte what the
seek-based ones produce. `write_block_group` was rewritten onto
`start_master_buf`/`end_master_buf` to eliminate ~4 seeks (and 4 MiB
BufWriter flushes) PER FRAME, which on an MPEG-2 title is ~350k frames — a
change only safe because the two encodings are identical.

Both write a FIXED-width 8-byte VINT placeholder (0x01 + 7 payload bytes)
and patch it in place; neither ever emits a minimal-width size. This test
pins that, so a future "optimisation" of either one to minimal-width sizes
cannot silently desync the two and change emitted Matroska.

## Test: `buffered_master_nests_correctly`

Nested masters must patch correctly too — the MVC BlockGroup nests
BlockAdditions > BlockMore inside the BlockGroup, and in-memory patching
works by index rather than by file offset, so nesting is where an
index-arithmetic slip would show up.

## Test: `fixed_width_vint8_is_big_endian_over_the_full_payload`

The fixed-width 8-octet VINT is `0x01` followed by the 56-bit VINT_DATA
payload in BIG-ENDIAN order (RFC 8794 section 4.4). Every payload octet is
distinct here, so a shifted, reversed or dropped octet is visible; the top
payload octets are unreachable through `end_master` without a
multi-terabyte buffer, which is why this is tested at the encoder.
