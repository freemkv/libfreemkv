# AACS MKB internals — relocated rationale

Supplementary notes for `src/aacs/mkb.rs` comments that exceed the
comment-guard's internal-comment cap. Each section is pointed to by a
short `// See docs/mkb.md — <topic>` comment at the relevant call site.

## Record-walker consolidation (`mkb_records`)

Yields `(offset, rec_type, rec_len)` for each record — a 4-byte header
(type byte + big-endian 24-bit length) then the body — stopping at the
`00 000000` end marker or a malformed/out-of-bounds length. It is lazy
(no body clone), so a find-one-record caller never materialises the
multi-MB cvalue table. `walk_mkb` and every MKB record walk in
`aacs::resolve`/`aacs::derive` are built on this, so the framing rules —
and any future fix to them — live in exactly one place. They had
previously drifted across six hand-rolled copies before being
consolidated here.

## cvalue record selection (`mkb_find_cvalues`)

The cvalue table is record type `0x05` (Media Key Data) on BOTH AACS 1.0
and AACS 2.x MKBs — its 16-byte cvalue entries are 1:1 with the 5-byte
Subset-Difference index entries in record `0x04` — the standard AACS MKB
layout.

On AACS 2.x in-drive UHD MKBs the `0x05` table is large (the full
subset-difference cvalue set: ~181k entries on a retail MKB, 1:1 with the
giant `0x04` index), while record `0x07` (Explicit Subset-Difference
Record) is a much smaller structure (~96 entries) and is NOT the cvalue
table. An earlier version of this function preferred `0x07`, which
under-tested the Subset-Difference walk on UHD discs and prevented the
DK→walk path from ever finding the matching uv. The selection MUST
therefore be `0x05`-first; `0x07` is only a fallback for
malformed/legacy MKBs that somehow lack a `0x05` record.

## BE24 length field test (`mkb_records_honors_the_high_byte_of_the_be24_length`)

The record length is a big-endian **24-bit** field, so the high byte
carries lengths of 64 KiB and up. The MKB records that matter most are
exactly that size — a real UHD cvalue table is `46_101 * 16` bytes and a
`0x2d` variant record is ~92 KiB — so a walker that dropped the high byte
would mis-frame every record of a real MKB from the first big one
onward, and every downstream key lookup would read the wrong bytes.

(The pre-existing high-byte test used total length `0x0110`, whose high
byte is ZERO — it exercised the middle byte only. This test puts a
non-zero value in the high byte.)

## Header-only record test (`mkb_records_yields_a_header_only_record_at_the_buffer_end`)

`rec_len == 4` is a well-formed HEADER-ONLY record (the minimum the
walker accepts), including one sitting at the very end of the buffer
with no bytes after it. Rejecting either — the `pos + 4` bound or the
`rec_len < 4` floor being off by one — silently drops the MKB's last
record, and "the record isn't there" is indistinguishable from "the disc
doesn't carry it".

## MKBType raw-bytes test (`mkb_type_raw_reads_all_four_body_bytes`)

`mkb_type_raw` reports the 32-bit MKBType field verbatim (`[C]`
§3.2.5.1.1 Table 3-2), including a value this build does not recognise —
the caller uses it to tell "unknown MKB generation" from "no Type record
at all". All four bytes must come from the record body; reading any of
them from the wrong offset yields a type that silently classifies as a
different AACS generation.

The recognised constants all share bytes with the `0x10` record-type
header byte (e.g. `MKB_21_CATEGORY_C` is `48 15 10 03`), so this test
uses a value with four distinct bytes, none of them `0x10`.
