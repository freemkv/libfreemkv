# `dirimage::encode` — descriptor encoder notes

Overflow rationale for comments in `src/dirimage/encode.rs` that would
otherwise exceed the comment-guard caps. Each section below is pointed to
by a short `//` comment at the relevant spot in the code.

## Why a pure function

`encode` is a pure function from `Layout` to sectors — nothing here touches
the filesystem. That is what makes it testable against the production
parser in `udf.rs`: the same bytes this module writes are read back and
checked by the real UDF reader, so the encoder and the parser can't drift
apart silently.

## Why UDF 1.02 with a single Type-1 partition map

UDF revision 1.02 with a single Type-1 partition map is deliberate: it is
the DVD-Video profile, it is the shape `read_filesystem` takes when
`num_partition_maps < 2`, and it avoids the UDF 2.50 Metadata Partition
entirely. That also means a synthetic image never exercises the Metadata
Partition path in `udf.rs` (`:946-991`).

## `finish_tag`: why `tag_loc` correctness matters

Getting `tag_loc` wrong (absolute vs. partition-relative) is the classic
reason a hand-built volume mounts nowhere: a driver that validates the tag
location rejects the descriptor outright. `desc_len` is the descriptor's
total length including the tag; the CRC covers `buf[16..desc_len]`.

## `encode_cs0`: why ASCII, not Latin-1, for the 8-bit form

`parse_udf_name` (`udf.rs:1467`) decodes a compression-8 name with
`from_utf8_lossy`, so a 0x80-0xFF byte — legal CS0 under Latin-1 — would
come back as U+FFFD. Every character above 0x7F therefore takes the 16-bit
(UTF-16BE) form, which that parser decodes correctly.

## `file_entry`: why tag 261, not the Extended File Entry (266)

Real BD-ROMs use the Extended File Entry (tag 266), but an EFE requires
UDF 2.00+, and this image declares 1.02, so `file_entry` writes the plain
File Entry (tag 261) instead. `udf.rs` reads both — the 261 field offsets
it uses (`l_ea` 168, `l_ad` 172, ADs at `176 + l_ea`) are the ones written
here.

## `push_fid`: why FIDs may span logical blocks

FIDs are packed with no inter-descriptor padding beyond the 4-byte
alignment the spec mandates, and they are allowed to span logical blocks —
which is also what `read_directory` (`udf.rs:1312`) assumes: it walks the
directory extent as one flat byte run and stops at the first non-257 tag,
so any block-alignment gap would truncate the directory.
