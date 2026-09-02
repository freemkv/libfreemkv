# CSS spec table tests — rationale and mutation notes

Background for the property tests in `src/css/tables.rs`'s `mod tests`. The
comments in that file point here for detail beyond the internal-comment cap.

## tab1_is_a_permutation

TAB1 is a bijection on 0..256. CSS uses it as an invertible output
permutation in `css_DecryptKey`'s chained-XOR rounds; if two inputs
collided, the key mangling would not be invertible.

Mutation: duplicate any value (e.g. set `TAB1[1] = TAB1[0]`) -> the "maps
two inputs" assert fires.

## tab1_known_spec_anchors

TAB1's fixed structural anchors from the CSS spec table: `TAB1[0x00] ==
0x33` and the inverse `TAB1[0x33] == 0x00`. These two entries are the
canonical first-row / inverse-lookup landmarks of the published CSS TAB1
and pin the table's orientation.

Grounding: CSS specification TAB1, row 0 col 0 = 0x33; index 0x33 (row 3
col 3) = 0x00.

Mutation: change the first literal `0x33` in TAB1 -> first assert fails.

## tab2_is_a_permutation

TAB2 is a permutation of 0..256 (it is the LFSR1 high-byte feedback
substitution). A non-bijective TAB2 would bias the LFSR1 keystream.

Mutation: set `TAB2[8] = 0x00` (collides with `TAB2[0]`) -> assert fires.

## tab3_matches_lfsr1_generating_formula

TAB3 is the CSS LFSR1 low-word table: the 8-value feedback block
`BASE = [0x00,0x24,0x49,0x6d,0x92,0xb6,0xdb,0xff]` repeated 64 times —
`TAB3[i] == BASE[i & 7]`. The high bits of the 9-bit index do not affect
the output (the LFSR1 step indexes with the full 9-bit low register but
only `& 7` matters). This pins all 512 entries to the published cipher's
table.

Mutation: flip any single byte in the TAB3 literal -> the formula check
fails at that index.

## tab4_bit_reversal

TAB4 is the exact bit-reversal of each byte (CSS uses it to permute LFSR0
bytes on seed and output). `TAB4[b]` reverses b's 8 bits MSB<->LSB.
Therefore it is also an involution: `TAB4[TAB4[b]] == b`.

Grounding: `TAB4[0x01]=0x80`, `TAB4[0x80]=0x01`, `TAB4[0x00]=0x00`,
`TAB4[0xFF]=0xFF`.

Mutation: set `TAB4[1] = 0x40` (not the reversal `0x80`) -> bit-reversal
check fails at index 1.

## tab4_is_a_permutation

TAB4 is a permutation (bit-reversal is bijective). Distinct from the
reversal test: a table that is "reversal except two swapped entries" would
still be a permutation, and a table that is "reversal except one
duplicated entry" would fail this but might pass a sampled reversal check
— the two tests pin different failure modes.

Mutation: set `TAB4[2] = TAB4[1]` -> permutation assert fires.

## tab5_permutation_anchors

TAB5 is also a permutation (complement of a bijection is a bijection) and
its own self-consistency landmark: `TAB5[0x00] == 0xFF` (`TAB4[0]^0xFF`)
and `TAB5[0xFF] == 0x00` (`TAB4[0xFF]^0xFF`). Pins orientation independent
of the complement-loop test.

Mutation: change the first TAB5 literal `0xff -> 0xfe` -> the landmark and
permutation checks both catch it.
