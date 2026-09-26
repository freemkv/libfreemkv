# MPEG audio framing and validation

MPEG-1/2/2.5 audio frames are assembled across PES boundaries and split when
one PES contains multiple frames. Header version, layer, bitrate, sample rate
and padding determine the frame size and sample duration. Timestamp anchors
and source provenance survive fragmentation. Invalid reserved header fields
are rejected; audio payload CRCs are not checked.

Free-format bitrate index zero remains a legal passthrough case at PES
granularity because its header does not signal a frame length. Before sync is
seen, nonsync payloads retain passthrough behavior; after sync, later PES
payloads are continuations. Partial EOF frames are discarded, not manufactured.
Buffering is capped at 1 MiB.

Rust tests cover every split point, multiple frames per PES, version/layer
frame-size tables, timestamps and invalid headers. The FFmpeg QA gate compares
decoded PCM after deliberately fragmented MP2 input.
