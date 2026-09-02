# FLAC elementary-stream decodability gate (`src/mux/codec/flac.rs`)

FLAC frames carry no length field, so a raw stream is delimited only by
sync-scanning + CRC validation. In freemkv, though, FLAC never arrives raw:
it comes from mp4/mkv, where each packet is exactly one container-delimited
FLAC frame (a complete, pre-delimited frame per packet). So this parser
is a per-packet gate, not a framer: every FLAC frame ends with a 16-bit CRC
(poly 0x8005, init 0, non-reflected) computed so the residue over the whole
frame — footer CRC included — is zero (per the FLAC format specification,
RFC 9639, frame footer). A nonzero residue is definitive corruption → drop
the frame (a silence gap, never a shift — each packet keeps its own PTS),
logged via the shared tally.

A packet that does not begin with the FLAC frame sync is not a delimited
frame we can validate, so it is passed through unchanged (never
false-dropped).
