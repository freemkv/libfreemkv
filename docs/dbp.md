# `labels::dbp`

## Implementation

Iterates `CpInfo::Utf8` constant-pool entries rather than raw
byte-scanning each class file. Equivalent label coverage (the literal
`TextField,...` strings live in the CP as Utf8 entries) with no
false-positive risk from method bytecode or attribute names that
happen to contain `TextField,`. Language / purpose / qualifier
classification lives in `super::vocab` so all Java-parser families
share one source of truth.

The parser ignores any prefix before the first `TextField,` occurrence
in a constant string — whatever string-pool ordering placed ahead of
it is irrelevant.

## `MAX_LABEL_BYTES` rationale

The label is an owned copy of a slice of a `CONSTANT_Utf8_info` entry,
whose `length` field is a `u16` (JVMS §4.4.7) — so a single crafted
constant contributes up to 65535 bytes, and the `u16` stream-number
keyspace admits 65536 of them per type.

Headroom: real dbp menu labels are short display names — "English Dolby
Atmos" (19 bytes), "Spanish 5.1 Dolby Digital" (25). The longest plausible
retail string ("Portuguese (Brazilian) 5.1 Dolby Digital Plus") is 45
bytes. 256 leaves >5x headroom over that, and any string past it is menu
geometry or padding, never a language name — `vocab::lang` would not
resolve it anyway.

## `MAX_LABELS_PER_TYPE` rationale

The keys come from `parse::<u16>()` on disc bytes, so all 65536 slots per
type are reachable; paired with `MAX_LABEL_BYTES` this bounds the whole
scan at 2 x 512 x 256 bytes.

Headroom: the BD STN_table admits at most 32 primary audio and 32 PG
streams per playlist, and dbp emits one menu TextField per stream. 512
leaves 16x headroom over the spec maximum.
