# `src/mux/codec/coding.rs`

## Spec references

ITU-T H.273 (CICP code points, shared elsewhere), ISO/IEC 13818-2 §6.3.10
(MPEG-2 picture coding extension: `top_field_first`, `repeat_first_field`,
`progressive_frame`), RFC 9559 §5.1.4.1.28 (Matroska `FieldOrder` element
0x9D).

## HDR10 unit scaling

`Hdr10Metadata` values are stored in their raw SEI integer units (not yet
scaled to the Matroska float domain); the muxer applies the H.265 → Matroska
unit conversion at emit time so the scaling lives in exactly one place.

Spec: Rec. ITU-T H.265 D.2.28 (Mastering Display Colour Volume, payloadType
137) and D.2.35 (Content Light Level Info, payloadType 144).
