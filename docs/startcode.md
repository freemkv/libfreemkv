# `BitReader` (mux/codec/startcode.rs)

Minimal MSB-first bit reader over an RBSP, for the leading fields of a coded
slice header (H.264 `first_mb_in_slice` + `slice_type`; HEVC
`slice_segment_header`).

It does NOT remove emulation-prevention bytes (`00 00 03`). Those can only
appear after two consecutive `0x00` bytes, which cannot occur within the
first Exp-Golomb codes of a slice header (a slice header never begins
`00 00`), so the leading fields this reader is used for decode correctly. A
caller reading deep enough into a header that `00 00 03` could appear must
de-emulate the RBSP first.
