# AAC ADTS framing and validation

ADTS syncframes are buffered across PES boundaries and split when one PES
contains multiple frames. Each output is raw AAC: the ADTS header and optional
CRC are removed, and AudioSpecificConfig is exposed for the container header.
A PES timestamp anchors its first frame; subsequent frames advance by the
header's sample count and sample rate. The first byte's provenance is retained.

Reserved sample rates and lengths shorter than the header are rejected. CRC
contents are not verified. Before ADTS sync is seen, nonsync payloads retain
the raw-AAC passthrough behavior; after sync, nonsync PES payloads are treated
as continuations. A partial EOF frame is not emitted. Discontinuities discard
a partial frame and restart timestamp anchoring. Buffering is capped at 1 MiB.

Rust tests exercise every split point, multiple frames per PES, CRC removal,
AudioSpecificConfig, discontinuities, timestamps and malformed headers. The
FFmpeg QA gate compares decoded PCM after deliberately fragmented input.
