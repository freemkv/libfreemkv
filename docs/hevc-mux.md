# HEVC Annex B muxer (`src/mux/hevc/mod.rs`) — extended notes

Overflow detail for comments capped by `ci/comment-guard.py`. Each section is
pointed to from a short `//` comment at the named item.

## Module overview

Writes a raw `.hevc` / `.h265` Annex B byte stream:
`00 00 00 01 | NAL_unit | 00 00 00 01 | NAL_unit | …` with no container
framing. On the first frame the muxer emits the codec_private's VPS, SPS, PPS
(parsed from a `HEVCDecoderConfigurationRecord` in `length-prefixed-in-hvcC`
form), then converts each PES frame's length-prefixed NAL units to Annex B
and writes them.

## `write_frame`

Input may be either:
- Length-prefixed: `[u32-BE len][NAL bytes]` repeated. This is the form
  emitted by libfreemkv's HEVC parser (the MKV-native layout). Converted to
  Annex B.
- Already Annex B: a buffer beginning with a `00 00 00 01` or `00 00 01`
  start code. Passed through unchanged.

## `hvcc_to_annex_b`

Layout (per ISO/IEC 14496-15 §8.3.3.1.2):
- 22-byte fixed header
- byte 22 = `numOfArrays`
- each array: `array_completeness:1 | reserved:1 | NAL_unit_type:6`,
  `numNalus:u16-BE`, then `numNalus` × `(nalUnitLength:u16-BE + NAL bytes)`.

We don't filter on NAL type — VPS (32), SPS (33), PPS (34), and any SEI
arrays included in hvcC all get the same Annex B treatment.

## `length_prefixed_to_annex_b`

Already-Annex-B input (a buffer beginning with a `00 00 00 01` or `00 00 01`
start code) is detected up front and passed through unchanged — some
upstream paths (raw HEVC ES from disc) hand Annex B straight through the
PES layer, and a genuine start code would otherwise be misread as a u32-BE
length prefix.

Truncation policy (single source of truth across all muxers): if a length
prefix runs past the end of the buffer (e.g. a NAL truncated by a bad disc
sector), the truncated trailing NAL is dropped and only the valid Annex-B
prefix accumulated so far is emitted. We never emit a half-NAL nor leak raw
length-prefixed bytes into the Annex-B stream.

## `nal_length_size`

Number of octets each NAL length prefix occupies in the elementary data of a
track described by `record` — the `lengthSizeMinusOne + 1` field of the
decoder configuration record (ISO/IEC 14496-15).

- avcC (`AVCDecoderConfigurationRecord`, §5.3.3.1.2): byte 4 is
  `bit(6) reserved | unsigned int(2) lengthSizeMinusOne`.
- hvcC (`HEVCDecoderConfigurationRecord`, §8.3.3.1.2): byte 21 is
  `constantFrameRate(2) | numTemporalLayers(3) | temporalIdNested(1) |
  lengthSizeMinusOne(2)`.

The spec permits only 1, 2 or 4 octets (`lengthSizeMinusOne` of 0, 1 or 3);
a declared 3 is non-conformant but is decoded rather than rejected, since
reading N octets is the same operation for every N. A record too short to
carry the field, or a codec with no such record, falls back to
`DEFAULT_NAL_LENGTH_SIZE` — the width every freemkv parser emits.

This exists because assuming 4 is silent corruption for a legal source:
reading a 2-octet-prefixed frame as u32-BE yields an absurd first length,
the conversion loop bails with nothing parsed, and the raw length-prefixed
bytes are passed through as though they were already Annex B — a stream
with no start codes at all, and no error anywhere.

## `append_length_prefixed_as_annex_b`

Same conversion as `length_prefixed_to_annex_b` but writes directly into a
caller-owned buffer, avoiding an intermediate allocation on hot paths (e.g.
per-frame video muxing). If `data` doesn't parse as length-prefixed (no
NALs extracted), it's appended unchanged on the assumption it's already
Annex B.

## `append_length_prefixed_as_annex_b_sized`

For a source whose NAL length prefixes are `length_size` octets wide rather
than the 4 this crate's own parsers emit. Derive `length_size` from the
track's configuration record with `nal_length_size` — ISO/IEC 14496-15 lets
a legal avcC/hvcC declare 1 or 2 octet prefixes, and reading those as
u32-BE mangles the frame.

`length_size` outside `1..=4` is clamped to `DEFAULT_NAL_LENGTH_SIZE`; the
field it comes from is 2 bits wide, so that is unreachable from real input.

## `avcc_to_annex_b`

Layout (per ISO/IEC 14496-15 §5.3.3.1.2):
- 5-byte fixed header
- byte 5 = `[reserved:3 | numOfSequenceParameterSets:5]`
- `numOfSPS` × `(sequenceParameterSetLength:u16-BE + SPS bytes)`
- 1 byte = `numOfPictureParameterSets`
- `numOfPPS` × `(pictureParameterSetLength:u16-BE + PPS bytes)`
