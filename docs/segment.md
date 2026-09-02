# AACS 2.1 FMTS forensic segment map (`src/aacs/segment.rs`)

Background moved out of the module's rustdoc to keep the doc comments within
the comment-guard's prose caps.

## Terminology

The **index** carried by each segment record (1..32) is NOT the AACS 2.1
*Media Key Variant* — that is the 65536-value device selector in the MKB
that decides *which set* of index keys a device receives, a layer this
module does not deal with. All the index keys belong to one variant, whose
number is unknown and irrelevant to the segment map. Decrypting a segment
with the ordinary CPS Unit Key yields garbage — broken HEVC reference
frames (empirically: `Could not find ref with POC …` on a plain unit-key
rip).

## Measurements from a retail AACS 2.1 disc

`index` is the 1..32 forensic index tag, NOT a sequential segment id:
measured on a retail 2.1 disc it cycles 1,2,…,32,1,2,… across records in
file order — 24 full cycles of 32 plus a final partial cycle of 24 = 792
records. Source-packet numbers are the 192-byte BDAV packet index: byte
offset = `spn * 192`. Each segment is ~2560 packets (~480 KB) = 80 aligned
units, spread across the entire 54 GB feature (one roughly every 67 MB).

Inside a segment the 80 units interleave in two stride-2 halves: applying
the segment's index key decrypts ~40 of them to clean TS and garbles the
other ~40 (a second interleaved half, unidentified), which the demux then
drops — leaving one coherent stream. Confirmed by decoding a retail disc
with a full set of 32 index keys.

## `index_to_key_idx`

The `fmts_key_ranges` callback maps a segment's `index` to a position in the
key pool, e.g. `|i| i as usize` when the pool is laid out as
`[base, idx1, idx2, …]`.

See also `docs/segment-key.md` for the on-disc key store these indices
select from.
