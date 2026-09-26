# FFmpeg QA validation

`.github/workflows/ffmpeg-validation.yml` is a reusable workflow called by
`qa.yml` on pushes to `qa`. It also supports manual runs on a side branch.
Normal development CI runs the Rust tests without installing FFmpeg.

The QA job installs Ubuntu's FFmpeg package, reads the crate's MSRV, and runs:

```sh
cargo test --release --lib ffmpeg_ -- --ignored --nocapture
```

All fixtures are synthetic and generated during the run: small test patterns,
sine waves, and a tiny PGS bitmap constructed in Rust. No ripped films or
external media corpus are downloaded. Set `FREEMKV_FFMPEG_ARTIFACT_DIR` locally
to retain fixtures. CI uploads generated media, commands, decoder/probe output,
hashes, tool versions and the test log for 14 days, including on failure.

## Coverage

- PGS visible/empty compositions at 180/183 and 3780/3783 seconds; decoded
  clears and warning-free FFmpeg stream-copy remuxing.
- H.264/AC-3, HEVC/E-AC-3 and MPEG-2 transport-to-MKV through the public mux
  driver, including reordered video pictures and delayed audio starts.
- AAC, MP2 and E-AC-3 transport audio discovery and remuxing.
- Fragmented AAC, MP2, AC-3 and E-AC-3 elementary streams: 137-byte input
  chunks deliberately split headers and audio frames.
- MKV roundtrips with variable frame cadence, multiple audio tracks,
  CodecDelay and DiscardPadding.

Audio/video tests compare stream metadata, decoded frame counts, all decoded
presentation timestamps with a shared origin shift (2 ms tolerance), and exact
SHA-256 hashes of decoded pixels/PCM. Generated output must decode without
warnings or errors. Hashes compare input/output in the same FFmpeg run rather
than a fixed encoder-version-dependent golden file.

The gate exposed and now guards fixes for lost PGS clears (also reproducing
issue #52's timestamp warnings), PES-granular AAC/MP2 parsing, missing generic
transport audio types, audio lost before a buffered MPEG-2 opening picture,
and dropped MKV decoder-delay/end-padding metadata. Pure Rust regressions run
in development CI; external decoder checks run explicitly in QA.

The gate is bounded, synthetic interoperability coverage. It does not replace
real-disc QA, player testing, or cover every codec/profile. Free-format MPEG
audio retains PES-granular handling. Container timing/padding preservation is
currently local MKV-to-MKV; existing network/stdio PES serialization does not
carry the added metadata. Rust callers constructing `PesFrame` literals must
initialize the new `discard_padding_ns` field to zero unless preserving trimming.

## FFmpeg licensing

Reviewed against [FFmpeg's legal guidance](https://ffmpeg.org/legal.html) and
[the GPLv2 text distributed by FFmpeg](https://raw.githubusercontent.com/FFmpeg/FFmpeg/master/COPYING.GPLv2),
section 0. FFmpeg is LGPL-2.1-or-later, with optional components that make a
build GPL; Ubuntu's software encoders can include those components. GPLv2
permits running the executable. Its output is not automatically GPL-covered
merely because FFmpeg produced it.

This workflow invokes separately installed `ffmpeg`/`ffprobe` executables.
It does not link FFmpeg libraries into libfreemkv, copy FFmpeg implementation
code, modify FFmpeg, bundle its binaries, or upload those binaries as artifacts.
Our generated media and diagnostic logs do not embed FFmpeg implementation
code. On that basis, this CI use does not trigger FFmpeg redistribution or
library-linking obligations for libfreemkv. Reassess if packaging/linking changes.

This is a software-license assessment, not a guarantee of patent clearance.
Codec patents are separate from FFmpeg's copyright licenses and depend on
jurisdiction and use; FFmpeg's legal page explicitly distinguishes them.
