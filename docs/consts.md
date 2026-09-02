# `consts::coding_type` — spec citations

`coding_type` is one registry used in two places that share the same value
space: the MPEG-TS PMT `stream_type` (ISO/IEC 13818-1 Table 2-34) and the
Blu-ray STN/CLPI `stream_coding_type` (BD-ROM Part 3).

The standardized video codes (`0x02`, `0x1B`, `0x24`) are ISO assignments
(ISO/IEC 13818-1 Table 2-34); `0xEA` (VC-1) is a BD-ROM convention in the ISO
user-private range. The `0x80..=0xA2` audio/graphics codes also sit in the
user-private range and follow the Blu-ray Disc Association / ATSC A/52
convention.
