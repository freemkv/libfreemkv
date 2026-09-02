# ADTS decodability gate

## Module rationale (`src/mux/codec/adts.rs`)

Per the ADTS framing defined in ISO/IEC 13818-7 / ISO/IEC 14496-3, a header
is structurally invalid in exactly three ways this gate treats as hard
rejects: syncword != 0xFFF, a reserved `sampling_frequency_index` (the sample
rate table has 13 valid entries, so index >= 13 is reserved), and
`aac_frame_length < 7` (shorter than the fixed+variable header itself). The
optional 16-bit ADTS CRC is not verified here — it is simply skipped. So the
gate enforces those three rejects: a packet that begins with the ADTS sync
but is otherwise malformed is dropped; a packet with no ADTS sync is raw AAC
(e.g. from an MP4 container, which carries no ADTS header) or a continuation
and passes through unchanged — never false-dropped. Raw AAC has no per-frame
integrity data, so like LPCM it cannot be gated.

## Test: `a_crc_present_header_shorter_than_its_own_crc_is_invalid`

A header that CLAIMS a CRC (protection_absent = 0) but declares a frame
length too short to contain one.

`aac_frame_length` counts the header and the CRC, not just the payload,
so with a CRC present the smallest structurally possible frame is 9
bytes: the 7-byte fixed+variable header plus the 16-bit crc_check.
The gate compared against a flat 7 and never read protection_absent at
all, so a frame whose own header says it is impossible was classified
Valid and forwarded to the muxer.

## Test: `dropped_frames_are_counted_but_their_duration_is_not_invented`

A dropped ADTS frame is dropped BECAUSE its header failed validation, so
the very fields a duration would come from (sampling_frequency_index, and
the 1024-samples-per-AAC-frame constant applied to it) are the ones known
to be untrustworthy. This gate therefore reports the drop's duration as
zero rather than deriving a number from a header it has just rejected —
the honest answer, and the one the count alongside it must be read with.
A nonzero constant here would report silence that was never measured.
