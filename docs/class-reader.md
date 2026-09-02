# `labels::class_reader`

Hand-rolled JVM `.class` file reader, tailored to the subset needed for BD-J
label extraction (Deluxe / dbp / similar frameworks). Implements JVMS §4
(class file format) and §6 (bytecode) only as far as: constant pool,
methods, the `Code` attribute, and a non-allocating bytecode iterator.

No external deps beyond `std`. No `unsafe`. No panics on malformed input —
every parse fault is a typed `Error`. This is shared infrastructure for any
label parser that needs structured access to `.class` files inside a
`/BDMV/JAR/<x>.jar`.

## `ConstantPool::from_entries`

Test-only constructor — builds a constant pool directly from a vector of
entries. Real callers go through `ClassFile::parse`, which builds this from
class-file bytes. Used by parser unit tests (e.g. `labels::deluxe`) that
need to exercise bytecode walkers against synthetic class fixtures without
hand-rolling valid `.class` byte buffers.

Caller is responsible for: prepending a `CpInfo::Empty` at index 0 (the
spec-reserved slot), and inserting a `CpInfo::Empty` after each Long/Double
entry (the 2-slot quirk).

## `decode_modified_utf8`

Decodes JVM "modified UTF-8" (JVMS §4.4.7). Practically identical to
standard UTF-8 for the BMP-printable subset seen in label strings, but with
two notable deviations:

- U+0000 is encoded as the two-byte sequence `0xC0 0x80`, not as `0x00`.
- Supplementary characters (U+10000..) are encoded as a UTF-16 surrogate
  pair, each surrogate emitted as 3-byte modified UTF-8.

For label data (mostly ASCII / Latin-1 / CJK in BMP), the simple
implementation here covers everything we'll encounter. The `0xC0 0x80` →
U+0000 case is tolerated explicitly; supplementary characters would need
surrogate-pair stitching, but no label-relevant string uses them.

## `Reader::slice` overflow test

`Reader::slice` takes an attacker-supplied length: a JVMS `u4`
`attribute_length` / `code_length` (§4.7, §4.7.3) or a `u2` Utf8 length
(§4.4.7). Adding it to `pos` without a wrap check panics on overflow in
debug and, in release, wraps to a small end offset that slips past the
bounds check and then panics inside the slice index. Both are panics
escaping a parser whose whole input is untrusted disc bytes; the contract
is an EOF error.
