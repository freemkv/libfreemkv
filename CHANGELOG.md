# Changelog

## [1.0.0-rc.6] — UNRELEASED

### Added

- **Every error is now `Error: E<code> <message>`, with an Error Codes
  reference.** User-facing errors show their code so you can look it up, and a
  new **Error Codes** page lists every code with its message, cause, and next
  steps. A contract test guarantees every error variant has a code, a message in
  all seven languages, and a Codes-page entry. Messages are source-agnostic
  ("key source", never a specific database).

### Fixed

- **Multi-extent AACS alignment in `dir://` extraction.** The decrypted
  file-tree extractor anchored the AACS unit grid to the first extent only, so a
  file spanning multiple clip extents could mis-align and corrupt. Each extent
  now anchors on its own encrypted-region start (the same class as the rc.5.2
  clip-anchor fix). Decryption math is unchanged.
- **Distinct "no key" reasons.** A disc whose key needs a Volume ID that
  couldn't be obtained now reports a distinct error from "no key at all".
- **autorip keydb writes go to the right path.** Auto-download, daily refresh,
  the "Update KEYDB" button, and the startup existence-check now resolve to the
  service's config path (matching where reads look); they previously used the
  CLI's executable-local default.
- **Crash-safety hardening** in `dir://` extraction and keydb writes (fsync of
  files and parent directories around rename).
- **Windows-reserved filenames** (`CON`, `NUL`, `COM1`…) inside a disc's file
  tree are safely renamed on extraction instead of aborting the walk.

### Tests

- 58 new tests across the toolchain (AACS key resolution, the unlocker seam, the
  key sources, DVD/CSS, `dir://` routing, and autorip keydb resolution).

## [1.0.0-rc.5.3]

### Added

- **`dir://` output** — write a decrypted `VIDEO_TS` / `BDMV` file tree straight
  from a disc or ISO instead of a single muxed file.

### Changed

- **Source-agnostic key errors** — decryption messages no longer assume a local
  key database is *the* key source.
- **The default `keydb.cfg` location is next to the executable** (portable CLI);
  the autorip service keeps its container path.
- **Simpler flags** — dropped `-k` (use `--keydb`) and removed `--device` (the
  drive is named in the source URL, e.g. `disc:///dev/sgN`).

### Fixed

- **Fail loud on missing keys or bad input** instead of silently writing an
  undecrypted file.

## [1.0.0-rc.5.2]

### Fixed

- **Windows Explorer now reports the full 25 fps for interlaced SD-DVD.**
  rc.5.1 added a `DefaultDecodedFieldDuration` (20 ms field) element to the
  576i/480i track header on the theory that Windows derives fps from it. The
  captured Silence-of-the-Lambs evidence proved the opposite: with
  `FlagInterlaced=1` + `DefaultDuration=40 ms` + `DefaultDecodedFieldDuration=20 ms`,
  Windows Explorer reported 12.5 fps (half) and MediaInfo flipped the track to
  "Frame rate mode: Variable". MakeMKV's correct rip of the same disc OMITS
  `DefaultDecodedFieldDuration`, keeps `FlagInterlaced=1` + `FieldOrder=TFF` +
  full-frame `DefaultDuration` (40 ms), and Explorer shows the full 25 fps with
  MediaInfo "Constant". The element is no longer written (`MkvTrack::video` now
  passes `field_duration_ns == 0`); the only frame-rate signal tools trust,
  `1/DefaultDuration` = 25 fps, is the full-frame value. Interlace signalling
  (`FlagInterlaced=1`, `FieldOrder=TFF`) is retained, and MediaInfo still
  reports "Interlaced / Top Field First" because it reads scan type from the
  MPEG-2 elementary stream's picture coding extension, not the container flag.
- **Correct AC-3 audio track selected on DVDs with non-standard sub-stream
  ordering.** freemkv assigned each declared audio stream a physical sub-stream
  by ordinal (`0x80+n`), assuming the IFO's first stream lives at `0x80`. On
  discs where the 5.1 main mix sits on a different sub-stream and `0x80` carries
  a 2.0 down-mix (e.g. Silence of the Lambs), the 2.0 was muxed under a "5.1"
  label. freemkv now probes each physical sub-stream's actual channel count from
  the disc — scanning every AC-3 frame and taking the maximum, so a brief 2.0
  logo bed at the feature head can't mask the real 5.1 — and routes each declared
  stream onto the sub-stream that genuinely matches.
- **"Decryption failed" on large AACS Blu-ray titles fixed.** The unit-alignment
  gate measured `lba % 3` against absolute disc LBA 0, but AACS aligned units are
  anchored at each clip's encrypted-region start. A clip whose start is not
  3-aligned had its readable units wrongly rejected — failing the feature/large
  titles of some discs while short clips passed. The gate is now clip-anchored.
- **Single-pass disc→MKV recovers marginal/transient sectors before failing.**
  The direct-to-MKV path now gives the drive its full ECC recovery budget on a
  bad sector (matching the multipass rip) instead of reporting a read failure a
  multipass rip would have recovered.
- **4K decode glitches at non-seamless clip joins fixed (Top Gun class).**
  Titles assembled from clips joined at non-seamless boundaries no longer drop
  reference frames at the join ("Could not find ref" stutter); the splice
  keyframe is rewritten so the decoder discards only the genuinely-dangling
  leading pictures.

### Changed

- **`freemkv-keysources` is now a pure key lookup.** The encrypted content-sample
  reader and the candidate-key resolution loop moved into libfreemkv (they read
  the disc and validate keys — decryption mechanism, not lookup). A key source
  now only looks a key up and hands it back. Downstream API: use
  `libfreemkv::read_encrypted_units` / `libfreemkv::resolve_and_apply` (was
  `freemkv_keysources::read_sample_units` / `…::resolve_and_apply`).

### Added

- **`--log-level 3` is now self-sufficient for MKV/opening-frame diagnosis.**
  The diagnostic pass now (a) dumps the ACTUAL MKV `TrackEntry` elements written
  per track (`tag=mkv.track`: FlagInterlaced, FieldOrder, DefaultDuration,
  DefaultDecodedFieldDuration via field-duration, Display dims, codecPrivate as
  hex) so the Windows-fps-class metadata is verifiable from a log alone, and
  (b) captures the first ~100 coded frames per track (raw bytes) to a
  `<output>.opening.bin` side file with a per-frame summary line
  (`tag=mkv.opening.frame`: track, key/delta, size, PTS) so opening-GOP / menu
  issues are diagnosable from a future log without the disc. Both are gated to
  log-level 3; a normal run opens no side file and records nothing.

### Verified

- **DVD opening-GOP / still-frame open handling is correct (no change needed).**
  The hypothesis that the opening pictures get the wrong (last-seen) sequence
  header or have their PTS floored to t=0 was traced and ruled out: the
  codecPrivate is the FIRST sequence header (read once at headers-ready, before
  any later AU), DVD VOBU structure guarantees each title opens on a sequence
  header + I-frame (no mid-GOP open), the parser back-anchors leading
  still-frames to the disc's real timeline, and the muxer anchors its timestamp
  base on the opening keyframe's real PTS so the t=0 floor never corrupts it.
  Regression tests pin all three.

## [1.0.0-rc.5.1]

### Fixed

- **CSS reads unlocked on enforcing drives.** CSS-protected DVDs on
  drives that enforce CSS authentication previously produced an empty MKV
  at exit 0, or hung indefinitely. The read path now issues the bus-auth
  handshake (`css::auth::unlock_css_reads`) to unlock scrambled-sector
  reads before attempting any data transfer, so the drive gates lift
  correctly.
- **Keyless title-key recovery always runs.** The Stevenson known-plaintext
  attack (`css::crack_key` / `src/css/stevenson.rs`) now recovers the
  title key even when the bus-auth scan detects a CSS drive, removing a
  code path that fell through to locked reads on certain disc/drive
  combinations. A wrong key still fails cleanly (confirmed by a sector
  descramble check) rather than producing silent garbage.
- **Early bail on undecryptable discs.** When CSS authentication succeeds
  but no valid title key can be recovered, the mux path now terminates
  with a clear error code instead of writing an empty (or zero-byte)
  output file.
- **DVD audio channel count from AC-3 bitstream.** The audio channel count
  is now parsed from the AC-3 elementary-stream bitfield rather than from
  the IFO audio attributes, so the reported channel count always matches the
  actual muxed audio even when the IFO attribute disagrees. Passthrough only
  — no downmix is performed. (Selecting the correct audio sub-stream on discs
  with non-standard ordering is a separate item — see Known issues.)
- **Interlaced MKV frame rate on Windows.** Interlaced content (576i/480i)
  now emits a `DefaultDecodedFieldDuration` element in the MKV track
  header, which Windows Media Foundation and Explorer use to derive the
  display frame rate. Without it, players reported an incorrect or zero
  frame rate on interlaced tracks.
- **Per-track `BPS` bitrate tags populated.** The `BPS` tag is written for
  each track so players and shell extensions (Windows Explorer, MPC-HC,
  etc.) can display the per-stream bitrate without reading the full file.
- **Interlaced field order corrected to TFF.** 576i tracks were written
  with a bottom-field-first (BFF) container flag that disagreed with the
  top-field-first order carried in the MPEG-2 stream; the MKV `FieldOrder`
  element now matches the stream (TFF) so deinterlacers use the correct
  field parity.
- **DVD first-play menu no longer prepended to the feature.** The title
  VOBS base sector was read from the VTS menu-VOBS pointer (`vtsm_vobs`,
  offset 0xC0) instead of the title-VOBS pointer (`vtstt_vobs`, 0xC4), so on
  a disc that authors a per-title menu the entire menu VOB — e.g. a studio
  first-play "the parental level has been set, press yes" prompt — was
  prepended to the movie and every cell extent shifted back. The rip now
  opens on the feature's first frame.

### Changed

- **AACS handshake skipped on DVDs.** The AACS authentication sequence is
  no longer attempted on DVD discs (it never applied to CSS-encrypted
  media); attempting it on a DVD drive was a no-op at best and surfaced
  spurious errors at worst.

### Added

- **Structured disc diagnostics at `--log-level 3`.** A new diagnostic
  pass emits structured log events at INFO level when the log level is 3
  or higher: DVD PGC/cell layout and IFO video/audio attributes; BD/UHD
  playlist, clip, and AACS metadata. Provides a single-command snapshot
  for diagnosing mux or authentication issues without instrumenting the
  source.
- **Reduced per-operation log spam.** Mux-read and seek operations are
  demoted to TRACE (were DEBUG); benign navigation-packet drops are
  summarized as a single counter at the end of the title rather than
  logged per-packet.

### Known issues

- **Wrong audio track on discs with non-standard substream ordering.**
  Audio sub-stream ids are assigned by per-codec ordinal rather than read
  from the IFO/PGC stream-number table, so a disc whose physical substream
  order diverges from the convention may select the wrong audio track
  (e.g. a 2.0 stream in place of 5.1). Diagnose with
  `freemkv info disc://… --log-level 3`; fix tracked for the next release.

## [1.0.0-rc.4.2]

### Fixed

- **Windows durability.** New platform-aware `io::fsync` module: directory
  fsync is a no-op on Windows (std cannot open a directory there, which
  logged a spurious warning on every mapfile write — including from the
  CLI), and a shared `file_durable` helper opens files read+write before
  `sync_all` so the flush succeeds on Windows, where `FlushFileBuffers`
  rejects a read-only handle with `ERROR_ACCESS_DENIED`.

## [1.0.0-rc.4] — UNRELEASED

An audit-driven round of correctness, durability, and Windows-transport
fixes. No API changes; behavior is more conservative on damaged media and
on partial decryption.

### Fixed

- **Decrypt-time loss is accounted for.** A partial AACS/CSS decryption
  failure can no longer pass as a perfect rip — skipped/undecryptable
  bytes are folded into the loss total — and partial CPS-unit (per-title)
  key coverage is rejected in the AACS validation gate instead of
  producing partly-garbage output.
- **Durable writes.** `keydb.cfg` is written atomically (temp file +
  fsync + rename), and the mapfile fsyncs its parent directory after the
  rename so a resume checkpoint survives a crash.
- **Truthful error causes.** A server-dropped keydb download is
  classified as a connection error, not a parse error; a missing home
  directory maps to "not found" rather than a keydb-parse failure; the
  I/O error from opening an AACS-inputs ISO is preserved; and a
  transport failure is preserved through the AACS auth handshake instead
  of being relabeled.
- A failed `READ CAPACITY` now warns instead of silently using a
  zero-sector disc.
- A leaked pipeline consumer can no longer finalize an abandoned output.
- **Windows SCSI.** `ScsiPassThroughDirect` is packed to match the
  `ntddscsi.h` layout, `StorageAdapterDescriptor.BusType` width is
  corrected (`u8` → `u32`), oversized read batches on non-sysfs
  (Windows) drives are bounded, `IOCTL_STORAGE_RESET_DEVICE` failures are
  surfaced, and a device reset only sleeps on success.
- Mux now tracks skipped bytes so a partly-read title reports accurate
  loss.

### Changed

- The per-read `Drive::read` trace event was demoted to TRACE so a debug
  log isn't flooded by per-sector reads.

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
