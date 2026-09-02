# m2ts_mux — standard MPEG-TS muxer

## Wiring

Single program (PMT PID `0x1000`, program number `1`):
  - PAT on PID `0x0000`
  - PMT on PID `0x1000`
  - Video (HEVC, stream_type `0x24`) on PID `0x0100`
  - First audio (AC3, stream_type `0x81`) on PID `0x0101`
    (or TrueHD, stream_type `0x83`, if hinted)

## Scope vs. full TS

This is a deliberately minimal viable muxer:
  - One program, one video track, optionally one audio track.
  - PAT + PMT re-emitted every `PSI_INTERVAL_PACKETS` packets so a
    mid-stream receiver can lock on within ~100 ms at typical
    UHD bitrates.
  - PCR clock derived from video PTS (`pts - PCR_LEAD_90KHZ`),
    attached to the video PID's adaptation field every
    `PCR_INTERVAL_PACKETS` packets.
  - No language / descriptor tags, no SCTE-35 markers, no per-PID
    PMT version bumps, no SDT/EIT. Sufficient for a conformant
    demuxer to play this back, not for full broadcast deployment.

## `base_relative_pts`

Convert input PTS (nanoseconds) to 90 kHz ticks rebased on the stream's PTS
origin. The origin is seeded ONLY by the first video frame
(`may_seed_base == true`); audio frames never seed it. This keeps the
audio/video offset intact: a leading audio frame can't pull the base up and
collapse the first/lowest-PTS video frame to 0. Frames earlier than the base
saturate to 0.

## `write_pes` packet-size math

Emit one PES payload as a chain of TS packets on `pid`. If `pcr` is provided
the first packet carries an adaptation field with the PCR. PAT/PMT are
re-emitted every `PSI_INTERVAL_PACKETS`.

TS = 188 bytes, header = 4 bytes, so 184 B remain after the header for the
adaptation-field area plus the payload area. With AF body of `b` bytes and
`s` stuffing bytes: AF total = `1 + b + s` (the leading `1` is the
`adaptation_field_length` byte itself). Payload = `184 - (1 + b + s)`. With
no AF area at all: payload = `184`.

The fit-the-tail logic on the last packet of the PES uses stuffing rather
than a separate small packet, which is the standard MPEG-TS convention.

## Test notes

### `audio_pes_header_is_byte_exact_and_bounded_unlike_video`

Golden vector for an audio PES header (ISO/IEC 13818-1 §2.4.3.7).

Audio and video differ in TWO fields that no downstream check in this module
distinguishes: the stream_id, and whether `PES_packet_length` is filled in or
left at the unbounded 0x0000 form. A receiver reads the length to find the
end of the access unit without scanning for the next start code, so an audio
PES that borrowed the video form (or emitted a stub header) still tiles into
valid-looking 188-byte packets and only fails inside a decoder.

### `absent_or_unparseable_codec_private_is_reported_not_silent`

The parameter-set prepend is attempted once, on the first keyframe, and the
latch is armed whether or not it produced anything. When it produced
nothing — no `codec_private` was set, or the `hvcC` will not parse — the
emitted TS carries no VPS/SPS/PPS at all and its video cannot be decoded, yet
`finish()` returns `Ok`. That must not be silent (same defect, and the same
fix, as the BD-TS sibling `tsmux.rs`): the muxer now warns and, so a caller
can act on it, exposes `parameter_sets_emitted()`.

Mutation check: drop the `params_emitted` bookkeeping and the two arms are
indistinguishable to any caller.

### `pcr_restamped_mid_pes_within_interval`

Regression: PCR must be re-stamped MID-PES, not only at PES boundaries. A
single large video frame (one PES) spans far more than PCR_INTERVAL_PACKETS
TS packets — a UHD I-frame. PCR-bearing video packets must recur at least
every PCR_INTERVAL_PACKETS video packets across that one PES; before the fix
only the PES's first packet carried PCR, leaving a multi-second clock gap
for the whole frame.

## `packet.rs` — single-packet builder + writer

`Packet` and `PacketWriter` are internal helpers (`pub(super)`) for this
module: `Packet` exposes raw byte layout so `mod.rs` can compose PSI / PCR /
PES bytes without each caller re-implementing the 188-byte boundary math;
`PacketWriter` writes each assembled packet straight through.

### `Packet::set_header`

Writes the 4-byte TS packet header from: `pid` (13-bit PID),
`payload_unit_start` (first packet of a PES / PSI section), `has_payload`
(packet carries any payload bytes), `has_adaptation` (packet carries an
adaptation field), `cc` (4-bit continuity counter).

### `Packet::append_adaptation`

`body` is the adaptation field body (flags byte plus optional PCR and so
on); `stuffing` is the number of `0xFF` stuffing bytes to append after the
body. The first byte of the field (`adaptation_field_length`) is computed
from `body.len() + stuffing`.

Returns `Error::M2tsPacketMalformed` if the computed
`adaptation_field_length` would exceed `MAX_AF_LEN` — the length byte and
the bytes actually written must always agree, so an over-long field is
rejected rather than written with a clamped (and therefore lying) length
byte.

### `PacketWriter::write_packet` / test `flush_delivers_the_buffered_packets_to_the_sink`

`PacketWriter` adds no buffering of its own, so the sink's buffer is the
only one — and `flush()` is the only thing that empties it. Skipping it
truncates the transport stream mid-packet: the final 188-byte packets never
reach the file, so the last PES of the title is lost and the stream ends on
a partial packet (ISO/IEC 13818-1 §2.4.3.2 requires whole 188-byte
packets).
