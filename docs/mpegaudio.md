# MPEG audio (MP1/MP2/MP3) decodability gate

## Module rationale (`src/mux/codec/mpegaudio.rs`)

Per ISO/IEC 11172-3 / ISO/IEC 13818-3, an MPEG-audio frame is validated by
header sanity + framing resync, not a payload CRC (the optional 16-bit CRC in
the header protects only the side-information and is absent unless the
protection bit says otherwise). The gate mirrors that header-only check and
ACCEPTS free-format (`bitrate_index == 0`) as a legal decodable mode — it
deliberately does NOT apply the stricter free-format reject that a full
decoder would (see the note at the `bitrate_index` check in `mpa_verdict`).
So the gate rejects only the truly invalid headers: a packet that begins with
the 11-bit MPEG-audio sync but whose version / layer / sample-rate fields (or
the reserved bitrate index 15) are reserved/invalid is undecodable → drop it
(a silence gap; each packet keeps its own PTS). A packet with no leading sync
is not a frame we can validate (raw payload / continuation), so it passes
through unchanged — never false-dropped.

## Test: `flush_adds_no_phantom_frame_after_the_last_real_packet`

This parser is self-framing at PES granularity: `parse` emits (or drops)
every packet immediately and buffers nothing, so end-of-stream has nothing
left to hand over. A `flush` that manufactured a frame would append a
zero-length block at PTS 0 AFTER a track that has already run to its real
end — a Matroska Block whose timestamp jumps backwards past every cluster
before it (RFC 9559 §5.1.3.2 Blocks are relative to their cluster's
timestamp; a phantom 0 lands in the wrong cluster entirely) and an empty
audio frame no decoder can consume.
