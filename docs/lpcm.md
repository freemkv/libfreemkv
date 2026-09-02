# BD/DVD LPCM wire format

BD LPCM PES packets (TS stream type 0x80) carry a 4-byte header on the
elementary-stream payload:

```
Bytes 0-1: audio frame number
Byte 2:    reserved
Byte 3:    quantization (bits 7-6), sample rate (bits 5-4), channel assignment (bits 3-0)
```

This header is part of the ES payload, so the BD parser must strip it.

DVD LPCM lives in private stream 1 (sub-stream 0xA0-0xA7). Its 7-byte
private sub-header (sub_id + frames + first-access-unit-ptr(2) + emphasis +
quant/freq + channels) is stripped by `PsDemuxer` while demuxing the
Program Stream. By the time a DVD LPCM `PesPacket` reaches this parser its
`data` is already raw PCM, so the parser must NOT strip any further bytes —
doing so drops one sample pair per PES and drifts the audio.

The two origins are distinguished by the `strip_header` flag: BD = strip,
DVD = leave intact. The raw PCM data is otherwise one complete audio frame
per PES; no framing is needed.

For MKV: both BD and DVD LPCM map to codec ID "A_PCM/INT/BIG" (big-endian).
DVD-Video LPCM is big-endian per the DVD-Video spec, and `mkv.rs` emits
"A_PCM/INT/BIG" unconditionally for `Codec::Lpcm` — there is no DVD/BD branch
and no "A_PCM/INT/LIT" path, so no byte-swap or alternate codec ID applies.
All frames are keyframes (uncompressed audio).
