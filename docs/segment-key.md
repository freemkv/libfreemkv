# AACS 2.1 segment key tables (`AACS/SegmentKeyNNNNN.tbl`)

One file per CPS unit (`SegmentKey00001.tbl`, ...). It is the on-disc key
store for the forensic variant segments mapped by `super::segment`. A device
does not read a segment key directly. It derives a 16-bit variant selector
from the Media Key Variant chain (see `super::variant`) and uses that
selector to index this table, which is how the device's position in the key
tree decides which variant it can decrypt (the traitor-tracing link).

## Container format (confirmed against a retail AACS 2.1 disc)

```text
  header (8 bytes):  u32 tag | u16 index_space | u16 record_size
  record[index_space]  (record_size bytes each)
```

On the reference disc: `index_space` = `0xffff` (the full 16-bit selector
space, 65536 records), `record_size` = `0x0218` = 536. Total
`8 + 65536 * 536 = 35,127,304` bytes, which matches the file exactly. Each
record begins with an 8-byte sub-header, then 528 bytes of encrypted key
material.

## Not yet reversed

The internal layout of a record's 528-byte payload, and how it maps onto the
segments of `super::segment`. One numeric coincidence worth noting for
whoever cracks it: the reference disc has 792 segments and `528 = 33 * 16`,
with `792 = 24 * 33`, so `33` appears on both sides. Until the mapping and
the key derivation are pinned, this module exposes only the confirmed
container: locate the record for a given 16-bit selector.
