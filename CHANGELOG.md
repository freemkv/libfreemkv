# Changelog

## [1.0.0-rc.2]

Second release candidate for 1.0. libfreemkv is the core library: disc scan,
multipass sector recovery, content decryption (CSS, AACS 1.0/2.0), and the
threaded mux pipeline that turns a disc or ISO into an MKV. This candidate adds
keyless DVD/CSS support and correct DVD video, on top of security and recovery
hardening.

### Added

- **Keyless DVD/CSS title-key recovery.** A CSS-protected DVD decrypts with no
  key database — the title key is recovered directly from the scrambled disc
  data via the Stevenson known-plaintext attack (ported from libdvdcss) and
  validated by descrambling a sector and confirming the known plaintext
  reappears, so a wrong key fails cleanly instead of producing silent garbage
  (`src/css/stevenson.rs`). `Disc::scan_image` recovers the same title key from
  a raw, still-scrambled CSS ISO, so a raw image can be muxed without
  pre-decryption.
- **MPEG-2 Program-Stream access-unit reassembler** (`src/mux/codec/mpeg2.rs`).
  Buffers elementary-stream bytes across PES packets and emits exactly one
  coded picture per MKV block, with presentation timestamps reconstructed from
  the stream — fixing corrupted DVD video. Bounded buffer so a malformed stream
  cannot exhaust memory.

### Changed

- Self-contained keyframes: the active param sets (HEVC VPS/SPS/PPS, H.264
  SPS/PPS, VC-1 sequence/entry headers) are re-asserted at every keyframe and
  any mid-title param-set change is emitted in-band, fixing whole-segment
  HEVC/H.264/VC-1 corruption when a source stops repeating or reverts a param
  set.
- Block timestamps use presentation order keyed on track type, so B-frame video
  (including a Dolby Vision enhancement layer) keeps its true presentation
  timestamps instead of decode-order timecodes.
- Mux unit alignment is scheme-aware (AACS vs CSS/none), so DVD extents are no
  longer rejected for unit misalignment.
- MKV output records `freemkv <version>` in the Muxing/Writing application
  fields, so every output file is traceable to its build.
- Subtitle `BlockDuration` values are scaled by the segment timecode scale, so
  display durations are correct when the scale is not 1 ms.
- The NOT_READY retry pause in the patch (Pass N) loop is halt-responsive: a
  stop request interrupts the drive-recovery wait immediately instead of
  blocking shutdown.
- Bounded the keydb decompressed-plaintext reader (caps a malformed or
  zip-bombed download).

### Fixed

- A `READ(10)` that returns GOOD status with a residual underrun is treated as a
  failed read (routed to retry) instead of committing stale buffer data —
  closing a silent-corruption hole in the sweep and patch paths.
- `raw_command` on Linux masks the `DRIVER_SENSE` bit before treating a result
  as an error, preventing false transport errors on commands that return sense
  alongside a GOOD response.
- `READ CAPACITY (10)` rejects the "capacity exceeds 32-bit" sentinel instead of
  silently wrapping to 0 and misreporting disc size.

### Security

- Content keys (CSS disc/title keys, AACS unit/volume keys) are redacted in log
  output (logged as `<redacted>` with a 1-byte fingerprint); a test guards
  against any key field being logged with a raw value.
- The macOS SCSI shim uses `posix_spawn` directly instead of `system()` / `sh
  -c`, eliminating a command-injection vector on the device-path string.

## [1.0.0-rc.1]

First release candidate for 1.0 — the first tagged 1.0 milestone of the core
library. Established the full feature set: multipass sector recovery, content
decryption (CSS, AACS 1.0/2.0) from `keydb.cfg`, disc parsing, and the threaded
mux pipeline (see "Pre-1.0 development" for the consolidated feature list).

## Pre-1.0 development

Versions 0.x were the iterative development series leading up to 1.0. The
highlights, condensed:

- **Multipass recovery engine.** Pass 1 sweeps the whole disc sequentially,
  tolerating bad sectors with an adaptive damage-jump algorithm (mark the bad
  range, keep going). Pass N retries the bad ranges with per-sector recovery
  timeouts, reverse-direction reads, and range bisection. A mapfile tracks
  per-sector state across passes so a rip can resume.
- **Drive and SCSI layer.** Single-shot, synchronous SG_IO transport on Linux
  (with IOKit on macOS and SPTI on Windows), full SCSI sense decoding, and
  drive enumeration / presence probes. Single-shot reads by design — recovery
  lives in the multipass orchestration, not inline in the read path.
- **Content decryption.** CSS for DVDs and AACS 1.0/2.0 for Blu-ray and UHD,
  with keys read from `keydb.cfg`. A single decrypting decorator wraps the
  sector source so decryption is one audited surface, and a resolved key is
  verified against disc content before it is applied.
- **Disc parsing.** UDF, MPLS/CLPI (Blu-ray), and IFO (DVD) parsing for title
  and extent assembly, with bounds checks on values derived from untrusted disc
  input. Canonical main-title selection picks the real feature over a
  play-all virtual playlist on branching discs.
- **Mux pipeline (the "highway").** A three-stage threaded pipeline —
  read+decrypt, demux, codec parse — with a recycled buffer pool, taking
  file-backed mux from ~60 MB/s to several hundred MB/s warm-cache. Codec
  parsers for HEVC, H.264, VC-1, MPEG-2, TrueHD, DTS(-HD), and PGS feed an
  EBML/Matroska writer.
- **I/O stack.** Bounded-cache writeback (`sync_file_range` +
  `posix_fadvise(DONTNEED)`) keeps the kernel dirty-page cache bounded on long
  sequential writes, and time-batched mapfile persistence keeps NFS-staged rips
  fast.
- **Library hygiene.** No user-facing English in the library — all errors are
  numeric codes handled by the application layer. A large spec-grounded,
  mutation-verified test suite guards the silent-corruption surfaces. Rust 2024
  edition; release builds use thin LTO.
