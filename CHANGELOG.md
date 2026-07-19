# Changelog

## [1.5.0] — UNRELEASED

### Added

- **MP4 input (`mp4://` as a source)** — a progressive-MP4 demuxer, the inverse
  of the muxer: it rebuilds the title + per-sample index from `moov`/`stbl` and
  streams samples back as PES frames, so `mp4://` now flows to **any** sink
  (`mp4:// mkv://`, `mp4:// audio://`, `mp4:// json://`, …). Round-trip is
  frame-exact (verified `iso:// mp4://` → `mp4:// mkv://` preserves every video
  and audio packet). Progressive MP4 only; fragmented (`moof`) is future work.
- **MP4 output is faststart by default** — `moov` is written *before* `mdat`, so
  the file plays over HTTP without pre-fetching the end. Done with zero extra I/O:
  a `moov`-sized hole (`round_up_4MB(16 B/sample × est_samples) + 4 MB`) is
  reserved up front, so sample offsets are fixed and `moov` is dropped into the
  hole at finish with a `free` box for the slack (falls back to moov-at-end only
  if the estimate is blown).
- **Native MP4 output (`mp4://`)** — a progressive ISO-BMFF muxer
  (`ftyp`+`mdat`+`moov`), so a disc goes straight to a play-everywhere `.mp4` in
  one decrypt pass, no ffmpeg round-trip. Carries HEVC / H.264 video (HDR10 colour
  via `colr`) and the audio MP4 supports — AC-3 (`dac3`), E-AC-3 (`dec3`), and
  DTS / DTS-HD (`dtsc`/`dtsh` + `ddts`) — with correct sample tables and B-frame
  composition offsets. It's a **compatibility export, not archival**: MP4 can't
  hold TrueHD, LPCM, or bitmap (PGS/VobSub) subtitles, so those are **excluded
  with a loud, itemized report — never a silent drop** (`mkv://` remains the
  keep-everything path). Verified byte-for-byte equivalent to `ffmpeg -c copy`:
  frame-exact video and audio (incl. a real DTS-HD MA 7.1 track, layout-exact),
  identical colour and duration, clean decode.

- **Five extraction sinks — dissect a title, don't just rip it.** New write-only
  destinations that pull one facet of a title out on its own:
  - **`video://dir/`** — every video track to its own file, as a raw elementary
    stream in the codec's native form (`.hevc`, `.h264`, `.vc1`, `.m2v`, `.obu`).
    No audio, no subtitles. Completes the per-track-class trio with `audio://` /
    `sub://`.
  - **`audio://dir/`** — every audio track to its own file in a directory, each in
    its native container (`.thd`, `.dts` / `.dtshd`, `.ac3`, `.eac3`, `.aac`,
    `.flac`). No video, no subtitles. BD/DVD LPCM has no container of its own and is
    written as headerless big-endian `.pcm`.
  - **`sub://dir/`** — every subtitle track to its own file: PGS as `.sup`, VobSub
    as a paired `.idx` + `.sub`, text subtitles as `.srt`. No video, no audio.
  - **`chapters://file`** — a title's chapter markers as a single sidecar, in the
    format the output extension selects: `.xml` (Matroska), `.txt` / `.ogm` (OGM
    simple), or `.vtt` (WebVTT).
  - **`json://file`** — one title's complete structure as JSON.

  `audio://` and `sub://` are the demux path with a **kind filter** (only their
  track class is opened and written). `chapters://` and `json://` are **scan-only**
  — they read nothing of the elementary streams, so they return in seconds.

- **Drop-on-undecodable audio — keep everything decodable, drop the frames that
  aren't.** A damaged audio access unit is now dropped rather than muxed as a
  decoder-choking glitch, and A/V sync is preserved: a drop becomes a silence
  gap, never a shift of the following audio, and every drop is logged. Detection
  is per-codec, each mirroring the format's own authoritative integrity check:
  **DTS** (ffmpeg's core-header validity parse), **AC-3 / E-AC-3** (native frame
  CRC-16), **FLAC** (whole-frame CRC-16), **MP2 / MP3** and **AAC-ADTS** (header
  sanity — raw AAC passes through), and **TrueHD / MLP** (major-sync CRC-16 + AU
  parity, verified against real ffmpeg output; because MLP state carries across
  access units, a corrupt AU is dropped *forward* to the next major sync). LPCM
  and video are excluded by design (no in-frame integrity data; inter-frame
  prediction). A shared tally counts and logs drops and, for a track that is
  mostly undecodable, drops the whole track. Detects only frames that are
  *structurally* undecodable — corruption of the sample payload inside an
  otherwise-valid frame (bad bit-allocation / XLL data) is source damage a
  container remux cannot distinguish from good data.

- **Forced subtitle detection from PGS content.** A PGS subtitle track is flagged
  `FlagForced` when it displays subtitles and every one carries the HDMV
  `forced_on_flag` — a dedicated forced/narrative track — read from the stream
  itself rather than the disc's vendor label metadata, so it works on discs that
  carry no such blob. `info -v` reports the same verdict (one shared classifier
  drives both the muxer and the scan-time probe), so `info` and a rip agree.

### Changed

- **`json://` emits the complete title model — lossless, not a summary.** Every
  field the scan resolved is serialized: video carries resolution (+ pixel
  dimensions, interlaced flag), frame rate (+ exact fraction), HDR, colour space,
  display aspect, and measured CICP; audio carries channel layout (+ count), sample
  rate (+ Hz), language, and editorial purpose; subtitles carry the forced flag and
  qualifier. The clip list and chapter names are included too.

### Fixed

- **TrueHD: a couple of transient errors can no longer poison a whole track.**
  A corrupt access unit drops forward to the next major sync, but only the
  individually-verified corruption feeds the whole-track drop verdict now; the
  resync run is collateral — so two scattered errors no longer discard an
  otherwise-good multi-hour track. The per-AU PTS rate is refined only from a
  CRC-validated major sync (a corrupt one can't shift the resumed audio), and the
  resync clears only on a validated major sync.
- **Free-format MPEG-audio** (`bitrate_index == 0`) is a legal, decodable mode and
  is no longer dropped.

## [1.4.5] — 2026-07-18

### Fixed

- **FMTS (AACS 2.1) forensic discs now mux to a clean, single-variant stream.** A
  forensic segment interleaves the local device group's variant with a foreign
  group's at the aligned-unit level. The mux decrypted only our half but left the
  foreign half in the buffer as ciphertext, on the assumption that the demuxer
  "drops untouched ciphertext cleanly." It does not — a foreign unit's bytes hit a
  tracked PID at the 192-byte stride, mis-parse, and trip the demux's concealed-gap
  keyframe resync, which discards good frames of ours around every segment (visible
  playback glitches). `AacsKeyMap::read_plan` now turns the map into the title's
  read plan: every default / CPS unit, plus inside a forensic segment **only our
  phase's units**. The foreign half is never read, decrypted, or handed to the
  demux. On a retail 4K UHD title this took concealed-gap resyncs from **349 → 0**
  and recovered ~2 GB of previously-dropped frames. Wired into **both** mux paths —
  the file-backed highway (`build_iso_pipeline`) and the inline live-drive
  `DiscStream` (`with_key_map`) — so single- and multi-pass FMTS rips are both clean.

### Changed

- **Key-bearing types redact their `Debug` output.** Every type that carries key
  material (device keys, processing keys, unit keys, media keys, VUKs, resolved
  chains, CSS/AACS state, …) now prints a `<redacted>` marker instead of the bytes,
  so no key can reach a log or panic message. Each is covered by a test asserting no
  key byte appears.
- **Hex parsing is centralized and case-insensitive.** A single set of canonical
  `0x`/`0X`-tolerant hex→integer parsers replaces scattered ad-hoc parsing (this is
  what silently dropped keydb device keys written with an uppercase `0X` prefix).
- **Internal-only public surface narrowed to `pub(crate)`, and duplicate
  `foo_with_X` methods collapsed to one** — no behavioral change, smaller API.

## [1.4.4] — 2026-07-17

### Fixed

- **Online key requests are no longer silently dropped on discs that yield few
  sample units.** The online key source refuses any request carrying fewer than
  `MIN_SAMPLE_UNITS` (8) encrypted-content samples — too few can match an
  incidental unit rather than the one asked about (a false positive, most acute on
  AACS 2.1 forensic-variant content). autorip gathered only 4, so every online
  lookup was skipped before it ever reached the key service and surfaced to the
  user as "key service down." autorip's sample count is now tied to
  `MIN_SAMPLE_UNITS` with a **compile-time floor**, so it can never regress below
  the minimum again.

### Changed

- **The online request is assembled from a proven-sufficient sample set.** New
  `DecodeSampleSet` (`libfreemkv::keysource`) wraps the content-unit samples and
  can only be constructed with at least `MIN_SAMPLE_UNITS` of them — so an online
  key request cannot be built from too few samples. The minimum is validated once,
  at construction, rather than by a runtime check a caller could forget.

## [1.4.3] — 2026-07-17

### Changed

- **`MIN_SAMPLE_UNITS` moved to the base crate.** The minimum sample count an
  online key request must carry now has a single definition in
  `libfreemkv::keysource`; `freemkv-keysources` re-exports it, so the online source
  and libfreemkv's own forensic query size their requests from one shared value.
- **The online unit-key reply is parsed as a list.** A response carries either a
  single Unit Key (ordinary disc) or the full ordered set (an AACS 2.1
  forensic-variant disc); the client accepts both and maps array position to
  forensic index.

### Added

- **Forensic-variant online query samples the anchor segment.** On an AACS 2.1
  forensic-variant disc the online key query draws its sample from the first
  forensic segment (index 1) — one canonical, deterministic sample — instead of an
  arbitrary segment.

## [1.4.2] — 2026-07-15

### Fixed

- **Mux no longer nulls decryptable video or storms the key server on a
  bad-encoded region.** 1.4.1 relaxed the decrypt gate but left the surrounding
  machinery in place. On a unit whose key *decrypted* but whose plaintext didn't
  reassemble to clean MPEG-TS, the read path still restored ciphertext, tallied
  loss, and re-asked the online key server (forever returning the same correct key)
  while the mux concealed the unit as NULL TS. The root cause: *"did a key produce
  clean TS?"* was used as the verdict *"did we decrypt?"* — they are not the same.
  A correct key can decrypt content with broken encoding; broken TS is a muxer
  concern, never a decrypt verdict.

### Changed

- **One decrypt authority; policy at the caller.** `decrypt_sectors` is now a
  pure decrypt: applies the CPS unit key in place, leaves plaintext, and reports
  unverified bytes. It never restores ciphertext, nulls, or re-fetches a key.
  Clean-TS status is only a key-*selection* hint (multi-CPS) or a read-*verify*
  signal (sweep/patch). Callers own the policy: the mux passes decrypted bytes
  through unconditionally (the demuxer handles bad TS); sweep/patch treat an
  unverified unit as a failed read and re-read it. Removes the decrypt-time
  ciphertext restore, the mux NULL-TS conceal loop, and the per-unit key-server
  refetch, plus the dead `aacs_unit_still_ciphertext` predicate.

- **Decrypt and TS-structure are now separate primitives.** AACS has no MAC;
  the only "did it decrypt?" signal is whether plaintext looks like MPEG-TS —
  a data-quality / key-selection question, not a decrypt verdict. The old
  `decrypt_unit(...) -> bool` is split into `decrypt_unit_raw` (pure crypto) and
  `is_clean_ts` (structural check), composed explicitly only where needed. The
  mux calls only `decrypt_unit_raw`.

- **Key-proof floor replaces the 75% supermajority.** The old proportion
  (≥75% of content packets synced) conflated *the key worked* with *the content
  is well-encoded*. `is_clean_ts` now requires `synced >= min(E, 4)` on
  **encrypted** packets (skipping packet 0 whose `0x47` is in the clear seed):
  four synced packets ≈ 1-in-4-billion false-positive; `min(E, 4)` scales to
  short fragment tails so they're never false-rejected. A unit is "opened" when
  a handful of packets prove the key — bad-encoded packets are the muxer's job.

## [1.4.1] — 2026-07-14

### Fixed

- **Mux no longer discards good video over a single defective packet.** AACS
  decryption required **every** content packet to be conformant MPEG-TS: one
  authored-bad packet (encoding defect, AACS 2.1 forensic-variant frame) made
  the mux conceal the **whole** 6144-byte aligned unit as NULL TS (up to 31/32
  good packets discarded, tallied as loss). On affected discs this produced
  false "corruption" over otherwise-perfect video (~466 MB concealed across two
  UHD titles). The gate is now a padding-aware **≥75% supermajority** of content
  packets restoring their `0x47` sync — no wrong key reaches this threshold
  (uniform-AES noise floor ≈ 256⁻ⁿ), but a minority of authored-bad packets
  still passes. Opened units flow through verbatim; the demuxer drops
  non-conforming packets on sync-loss. TS-sync conformance is a muxer concern,
  never a decrypt verdict. (The supermajority threshold is tightened in 1.4.2.)
- **MVC (Blu-ray 3D) track signals unified and hardened.** The `mvcC`
  `CodecPrivate` extension, the `BlockAdditionMapping`, and each frame's
  `BlockAdditional` now all derive from a single `MVCDecoderConfigurationRecord`
  built once per track, so they can no longer diverge. A track is flagged 3D
  only when that record actually builds — a malformed dependent-view parameter
  set no longer emits a mapping with no matching record (previously the flag was
  taken from `mvc_params.is_some()`, which could orphan a `BlockAddID`). The base
  track's `CodecPrivate` now carries the `mvcC` extension block
  (`avcC ‖ u32be(size) ‖ "mvcC" ‖ record`, Matroska-spec size = block − 4) so
  players and mediainfo detect MVC at the track level.

## [1.4.0] — 2026-07-13

### Added

- **Blu-ray 3D (MVC) support.** A 3D disc now rips to an MKV that preserves
  **both eyes** as a single MVC video track — the AVC base (left) view in each
  Block, and the MVC dependent (right-eye) view as a per-frame `BlockAdditional`
  under an `mvcC` `BlockAdditionMapping` (`MVCDecoderConfigurationRecord` per
  ISO/IEC 14496-15 §7.6.2), paired to the base by PTS. Remux only — no
  transcode, no side-by-side conversion. The Blu-ray scan reads the interleaved
  `STREAM/SSIF/<clip>.ssif`, enumerates the dependent view (stream_type `0x20`)
  by the BD-3D PID convention, and parses it in a parameter-set-passthrough mode
  so every dependent frame is a self-contained access unit. Verified on
  *300: Rise of an Empire*: one MVC track, ~8.7 GB dependent payload carried in
  per-frame BlockAdditionals, base view byte-identical to the 2D rip.

## [1.3.2] — 2026-07-10

### Added

- **AACS 2.1 (FMTS) variant-decode foundation.** `UnitKey` gains a
  `variant_number` field (`0` = ordinary content, `1..=32` = a forensic
  variant) with `UnitKey::new` / `UnitKey::variant` constructors, and a new
  `aacs::variant_select` module resolves a disc's single forensic variant and
  classifies each aligned unit — decrypt with the default key, decrypt with the
  variant key, drop a foreign variant, or conceal a keyless forensic unit. This
  is the groundwork for selecting one variant's segments and dropping the other
  31; the decrypt-pipeline wiring lands with the variant key source.

### Fixed

- **`IndividualSegment.tbl`: the per-record field is the variant, not a segment
  number.** `Segment.number` → `Segment.variant`. Verified against a retail 2.1
  disc, the field cycles `1..=32` across the table (a per-variant tag) rather
  than counting up, so variant selection routes on the correct value.

## [1.3.1] — 2026-07-10

### Licensing

- **Relicensed to the MIT License, from 1.3.1 onwards** (releases up to and
  including 1.3.0 remain under AGPL-3.0).

### Added

- **Authoritative HD-DVD title composition** from the Advanced-Content playlist
  (`ADV_OBJ/VPLST000.XPL`): each title's clips, real duration, display name, and
  chapters come from the disc's own playlist instead of a clip-name heuristic. A
  layer-break split (`FEATURE_1`+`FEATURE_2`, `feature`/`feature_Divide`) composes
  into ONE title with the two parts as clips and their title-time offsets. Falls
  back to the clip-name heuristic when no playlist is present.

## [1.3.0] — 2026-07-08

### Added

- **AACS 2.1 (FMTS) is a first-class disc format.** FMTS discs are detected,
  labeled, and scanned as their own format rather than misread as plain UHD. The
  forensic variant segments are located from `IndividualSegment.tbl` and the
  `SegmentKey.tbl` container is parsed; the bulk of the title decrypts with the
  unit key as usual, and the forensic segments (for which no segment-key source
  exists yet) are skipped as expected loss, so a 2.1 disc rips mostly-complete
  instead of failing outright.
- **AACS 2.1 variant Media Key chain runs end to end.** The variant media key is
  derived as a clean Processing-Key to media-key primitive, with the record
  layout pinned against reference variant MKBs — the per-slot `C` block from the
  `0x0c` cvalue table, the `VARIANTS` table plus trailing nonce at `0x2d`, and
  `VKD` at `0x2f` — so a genuine variant MKB resolves through the ladder.
- **Partial HD-DVD support.** HD-DVD is detected as its own format and its
  `HVDVD_TS` `.evo` clips mux through the pipeline: EVO video is demuxed from the
  MPEG program stream, including VC-1 titles carried on extended stream id `0xFD`
  (real selector in the PES `stream_id_extension`), with the VC-1 access units
  reframed so each I-frame keeps its preceding sequence and entry-point headers.
  Title composition is heuristic for now (authoritative program-chain parsing is
  planned), so a disc that authors two distinct features under the layer-break
  naming may present them as one title.
- **Display-order timestamps for program-stream H.264 / VC-1 / HEVC.** A program
  stream stamps a PES PTS only once per GOP; the parsers now reconstruct a
  display-order PTS per frame from the coded picture type and the sparse anchor
  (duration self-calibrated from anchor spacing), so a decoder no longer sees
  colliding DTS. Gated to the program-stream path — the BD/UHD transport path
  (per-frame PTS) is unchanged.
- **Stream-label parsers: reader-backed detection and a menu-language fallback.**
  Label detection can inspect a jar's contents, so vendor parsers claim only
  their own discs; a new last-resort parser reads menu-artwork languages.
- **keydb round-trips AACS 2.0 host certs** (the `HC2` line) so a load/save cycle
  no longer drops v2 host credentials.

### Changed

- **MPEG-2 reassembles through the shared `AuAssembler`.** The MPEG-2 parser's
  hand-rolled PES buffer and offset-keyed mark queues are replaced by the same
  access-unit assembler the H.264/HEVC/VC-1 parsers use (in a new MPEG-2 mode);
  the GOP-buffered `temporal_reference` reorder and PTS origin-locking are
  unchanged, so DVD output is identical.
- **Generic per-scheme recovery seam.** Decrypt-miss handling is now a
  scheme-neutral seam the input stream installs (no recovery, or an AACS
  fresh-key fetch), with CSS self-recovering separately from the data itself. An
  undecryptable unit is counted the same whatever the scheme, so the separate
  "undecryptable" loss bucket folds into one loss count.
- **`aacs` module reorganized.** The former god-module is split into
  `media_key` / `volume_key` / `inf` / `resolve` / `mkb` / `crypto` / `content`,
  the `boil` veneer is removed, and module paths (not a `mod.rs` facade) are the
  public API.

### Fixed

- **Main title is chosen by largest physical size, not clip count**, so a
  chapter-per-clip disc (e.g. Fast & Furious) is no longer mis-ranked behind a
  virtual composite.
- **A fresh-rip ISO `sync_all` failure is no longer swallowed**: `is_regular` is
  read from the open file handle instead of a pre-create `metadata(path)` that
  always failed on a path that does not exist yet.
- **A transient CLIP-info parse failure no longer suppresses a clip's extents**
  for a later playlist item that references the same clip.
- **Reverify downgrades that fail to persist are logged, not swallowed**, so a
  bad unit cannot be silently mismarked good on resume.
- **The CLI sanitizes on-disc metadata** (title, volume label, playlist, stream
  labels) before printing, so a crafted disc cannot inject terminal escape
  sequences.
- **keydb entry validation matches the parser exactly** — a `0x` line counts only
  with a ` = ` — so content that parses to zero usable entries can no longer be
  saved as valid.
- **autorip** recovers a poisoned config lock in the rip thread instead of
  panicking it, and corrects the resume pass count.
- Criterion stream numbering (a map value of 0 no longer shadows stream 1); AACS
  resolve classifies a media-keys-only source missing the VID as "VID
  unavailable"; a dropped partial PES flags a discontinuity; the no-demuxer path
  detects an early consumer disconnect.

### Performance

- **Decrypt thread count is resolved once** and cached off the per-buffer hot
  path (the env var and `available_parallelism` are no longer probed per call).

## [1.2.2] — 2026-07-04

### Added

- **AACS 2.1 Media Key Variant support.** The Media Key Variant scheme is now
  detected and parsed from the real MKB record types found on variant discs —
  `0x2d` (Encrypted Media Key Variant Data), `0x2f` (Variant Key Data table,
  65,535 × 16), and `0x0c` (variant cvalues, one per subset-difference slot) —
  replacing the earlier placeholder `0x82`/`0x83` types, which were a guess and
  appear on no real MKB. The V2.0→V2.1 upgrade detection and fixtures are updated
  accordingly, so a genuine AACS 2.1 variant disc now resolves.
- **`resolve_candidate`** — one composed, pure-derivation boil-down for a
  candidate key at any ladder rung (DK/PK/MK/VUK → terminal unit keys), parsing
  `Unit_Key_RO.inf` at the disc's declared AACS version and returning every CPS
  unit key. Consumers stop re-composing the ladder; every client hardens a single
  implementation.

### Fixed

- **`mk_from_dk` does the real Subset-Difference walk again.** It previously ran
  the Media-Key-Variant path, which needed an integrator KCD absent in-tree and
  errored for every real disc — effectively dead for both consumers. It now
  performs the genuine device-key SD walk; the Volume ID enters at the VUK step
  (where it belongs), not the MK step. This revives the DK→MK fallback across the
  toolchain (`freemkv-keysources` adopts the corrected two-argument call).
- **autorip: a down online key service is no longer reported as a missing key.**
  When the online key source resolves no key for an encrypted disc, autorip now
  runs one bounded reachability probe (SSRF-pinned, ~8 s, no redirects) and
  distinguishes a transient outage (transport error / 502·503·504 → down; 429 →
  rate-limited) from a genuine no-key (any real HTTP answer → up). A transient
  verdict triggers a bounded key-resolution retry (3 attempts, 8/16/32 s backoff)
  and, if the service stays down, parks the disc in a distinct retryable state
  ("Key service unavailable — temporary outage, not a missing key; will retry.")
  instead of the permanent "no keys found". Never hammers the drive or service.

### Performance

- **Processing-Key resolution is ~15× faster on UHD.** A Processing Key is the
  key at its subset-difference node (one AES-G from the Media Key), so it is now
  tried directly against the MKB cvalue tables (direct PK × cvalue iteration)
  instead of BFS-walking the SD tree at unknown depth — which was both wrong for
  terminal PKs and slow on a large UHD MKB (~181k cvalues). PK derivation on UHD
  drops from ~37 s to ~2.4 s; the SD tree walk now lives solely in the device-key
  path.

### autorip

- **Clear stuck move errors from the System tab.** Each move-queue error now has
  a ✕ to dismiss it, plus Clear all and Refresh — so a resolved or stale error
  can be cleared without restarting the container (the mover re-records any that
  are still genuinely failing on its next tick).

## [1.2.1] — 2026-07-02

### Fixed

- **DVD DTS audio no longer muxes with non-monotonic timestamps.** A DVD
  Program Stream packs several DTS core frames into one PES packet; the parser
  stamped every access unit with that single PES timestamp and no per-frame
  duration, so consecutive frames collided on one PTS and a strict decode/remux
  (ffmpeg) rejected the track — `non monotonically increasing dts to muxer`.
  The DTS parser now derives each core frame's duration from its header
  (`(NBLKS+1)*32` samples ÷ the `SFREQ` sample rate) and re-bases to each PES's
  own container timestamp, advancing by a frame duration only *within* a single
  PES — so the track stays monotonic and does not drift past its real length on
  a feature-long title. The UHD DTS-HD MA path (one access unit per PES) is
  unaffected: each unit keeps its own PES timestamp, preserving the 1.2.0 per-PES
  attribution. Completes the DVD DTS fix begun in 1.2.0 (which corrected the
  silent-track routing, exposing this timing bug). Note: genuinely corrupt
  source DTS frames — valid framing, bad audio blocks — are passed through
  faithfully; freemkv never fabricates or drops audio it can't prove is bad.

## [1.2.0] — 2026-07-01

### Breaking

The disc's AACS version is now carried through the key-resolution path as the
single source of truth for the `Unit_Key_RO` stride (AACS-1.0 = 48-byte,
AACS-2.x = 64-byte), so keys are always read at the disc's own layout. That
threaded one new value through three public signatures. In-tree consumers
(`freemkv`, `autorip`, `freemkv-keysources`) are updated; external callers must
adjust:

- **`DiscInputs` gains a `version: u8` field** (between `volume_id` and `mkb`).
  Code constructing it with a struct literal must add the field. It is normally
  obtained from `Disc::inputs()`, not constructed by hand.
- **`keysource::DiscInputsCtx::new` takes one argument, not two** — the version
  is now read from `inputs.version` (`new(inputs)` instead of
  `new(inputs, version)`).
- **`disc::read_aacs_inputs` / `read_aacs_inputs_from_drive` return a 3-tuple**
  `(inf, mkb, version)` instead of `(inf, mkb)`.
- **`PassProgress` is no longer `Copy` and gains a `located: LocatedProgress`
  field.** It now carries a `Vec` (the rendered bad-range drilldown), so it's
  `Clone` only — still built once per throttled emission and passed by reference
  to `Progress::report`. Struct-literal constructors must add the field (empty:
  `located: Default::default()`). New public types `LocatedRange` /
  `LocatedProgress`.

These are source-breaking for external crates.io consumers. Shipped under a
minor bump (1.2.0): libfreemkv's surface is not yet frozen and the only known
consumers are the in-tree toolchain crates.

### Added

- **Pass-N marginal-sector recovery specialists.** The patch pass gained a
  roster of parameterized recovery techniques — read speed (max/min), cache
  bypass (FUA), and traversal (linear fwd/rev, bisect, cache-prime, oscillate,
  per-sector speed-sweep) — each targeting a distinct physical failure mode of
  marginal media. A per-rip **decayed (EWMA) scorecard** grades every technique
  by its recent recovery rate and re-orders them best-first, so the engine
  hardcodes no conclusion: a technique that fits *this* disc floats to the front
  and one that doesn't self-deprioritises (but is never dropped). Every read is
  wedge-safe and deadline-bounded; the existing fast/deep recovery behavior is
  unchanged (the specialists are additive, tried only on the hardened residue).
- **Opt-in flat-pool recovery scheduler (`FREEMKV_PATCH_FLAT`).** Collapses the
  breadth-first recovery tiers into one flat pool so every technique gets a shot
  at each bad range immediately, scorecard-ordered — a data-driven bandit for a
  hardened residual (e.g. a late resume) where the tiered ladder would spend a
  long time on cheap techniques before reaching the specialists. Unset keeps the
  proven tier ladder as the default.
- **`PassProgress` is the complete, mapfile-free progress contract.** Every
  emission now carries the fully-rendered "where is the damage" drilldown
  (`located`): the bad ranges annotated with chapter + movie-time offset, the
  main-feature at-risk time, the section count and the largest gap — computed by
  the library from its in-memory mapfile + title. A client (autorip, a future
  GUI/CLI) renders the disc map + at-risk time straight from it and never parses
  the mapfile, so a mapfile→mapdb change is invisible to clients. Adds
  `disc::locate_ranges`, the one-shot `disc::progress_snapshot_from_mapfile`
  (builds a snapshot from a mapfile on disk so a boundary/verdict paint stays
  mapfile-free client-side), and `consts::MILLIS_PER_SEC`.

- **`PatchOptions::fast_capture` — breadth-first patch recovery.** A fast-capture
  pass reads each bad range once at the full batch and leaves every failed block
  `NonTrimmed` for a later pass — no bisect, no re-read, no per-sector grind — so
  a first retry pass grabs the readable blocks (a sweep's good skip-ahead
  overshoot) of EVERY range before any single range's slow per-sector recovery.
  No data is dropped: a failed block stays `NonTrimmed` (retried by a granular
  pass), never `Unreadable`. A transport fault still aborts. `Disc::copy`'s
  internal patch leaves it `false` (single-call full recovery).

- **Mux loss concealment — a logged gap still produces a decode-clean file.**
  When a unit genuinely cannot be decrypted on the mux read path (a key the disc
  never yielded, after the rip's own decrypt-verify already failed loud and
  re-read), the mux no longer passes ciphertext downstream or emits a broken
  frame. The undecryptable aligned unit is concealed as NULL transport-stream
  packets (PID 0x1FFF, invisible to every real stream), and the codec layer
  **drops forward to the next keyframe** so no frame with a dangling reference
  reaches the muxer. An ffmpeg deep scan of the result is clean — no missing
  references, no partial frames. The loss is tallied and logged, never silently
  dropped, and the mux always completes. Audio and subtitle tracks have no
  cross-frame references, so only the directly-affected frames are dropped there.
  Decrypt-verify remains a **rip** gate (fail loud → re-read), never a mux gate.
- **`Disc::unlocker_matrix()` — registry-driven unlocker did-work report.** Returns
  each registered unlocker's name alongside a `did_work` flag recording whether it
  performed authentication steps during the current rip. Callers (autorip, the CLI)
  surface this so an operator can confirm at a glance which unlock paths —
  LibreDrive firmware, AACS, CSS — actually ran, with no hardcoded names on the
  caller side.

### Changed

- **One hex parser.** All hex parsing (keys, IDs, key-source inputs) routes
  through a single `libfreemkv::hex` parser instead of several ad-hoc decoders,
  so length/odd-nibble/invalid-digit handling is identical everywhere.
- **Robust encrypted-unit sampling + a single MKB framing walker.** Up-front
  AACS sampling tolerates content layouts that previously yielded too few
  encrypted units to resolve a key, and the Media Key Block is now walked by one
  framing routine shared across the in-band and out-of-band readers (no
  divergent record-stride logic). AACS resolution hardened around these paths.
- **One reader, one `DiscInputs`.** `Disc::inputs()` is now the single, complete
  source of a disc's AACS inputs (inf, MKB, VID, disc_hash, version), and
  `read_aacs_inputs*` returns the version alongside inf+MKB. Both the CLI and
  autorip resolve through `Disc::inputs()`; the duplicate out-of-band readers
  (autorip's `key_files()`/`volume_id()`) and the stale mapfile-VID read are
  removed. AACS file paths and the AACS major versions are now named constants
  (`aacs::PATH_*`, `aacs::AACS_MAJOR_*`, `AacsVersion::major`/`from_major`) so a
  fallback or stride change lives in exactly one place.
- **Pass-N recovery rebuilt as a bounded, never-hang handler chain.** The 1.1.0
  patch loop retried each bad range sector-by-sector until a per-range budget was
  exhausted, with no escape from a wedged drive short of the watchdog firing after
  tens of minutes. 1.2.0 replaces that with a two-tier handler chain dispatched
  breadth-first, largest bad range first:
  - **Jump** (lead tier): reads each range in large forward-skipping batches to
    quickly locate readable islands — clearing a multi-gigabyte dead spot in
    seconds rather than sector-by-sector.
  - **Bisect** (trailing tier): binary-searches the boundaries of each remaining
    bad block, converging to within a single sector of the last-readable LBA.
    Boundary-probe reads are exempt from the early-yield stall so the boundary
    walk always completes.
  - **Handler scorecard**: handlers that make progress stay at the front of the
    rotation per rip; an idle handler is ranked last so proven performers lead.
  - **Wedge detection**: a pass-level streak counter tracks consecutive
    wedge-family senses (HARDWARE ERROR / ILLEGAL REQUEST) across section
    boundaries. At the threshold the pass aborts and a soft un-wedge
    (`Drive::spin_cycle()` — START STOP UNIT, no eject) runs before the next retry
    pass, instead of grinding at near-zero throughput until the pass watchdog
    fires.

  No data is dropped: a block that neither handler recovers in a pass stays
  `NonTrimmed` for the next pass.

### Fixed

- **DVD DTS/LPCM audio tracks no longer mux silent.** On DVD-Video the
  `private_stream_1` sub-stream id's low nibble is the audio-stream *number*
  (shared across codecs), not a per-codec ordinal. A DTS or LPCM track that
  wasn't the disc's first audio stream got a sub-id one too low, so the demux
  routing key (`0xBD00 | sub_id`) never matched and every packet was dropped —
  the track appeared in the container but played silent (AC-3 at position 0
  worked by coincidence). Audio sub-stream ids are now assigned by positional
  stream number, so a DTS 5.0 track after an AC-3 5.1 track routes correctly.
- **ISO mux no longer drops real video at content-fragment tails.** A title's
  encrypted content can end mid-AACS-unit, with the disc zero-padding the rest
  of the 6144-byte aligned unit to the next fragment. The decrypt-verify
  demanded the TS sync byte on *all 32* source packets, so it rejected such a
  tail unit over its legitimate padding — discarding the real video packets it
  contained. On a flawless rip this surfaced as a small phantom "loss" at mux
  (and, once retries were exhausted, a truncated MKV). Unit acceptance is now
  **padding-aware**: only packets whose *source* (pre-decrypt) bytes are
  non-zero must restore their TS sync; the zero padding is excluded from the
  check and emitted as clean zeros. A full content unit still requires all 32
  (unchanged — no wrong-key relaxation), and a unit whose *non-zero* tail fails
  to decrypt is still rejected as a genuine bad read.
- **ISO online key resolution now sends the Media Key Block.** Capturing a
  disc's AACS inputs at scan read the MKB with a full `read_file` of the
  ~128 MiB `MKB_RO`/`MKB_RW` allocation, which fails on file-backed readers —
  leaving the MKB empty, so `Disc::inputs()` shipped `mkb=0` to an online key
  service and the request was rejected (no key → no decrypt). Scan now reads the
  MKB through the same bounded prefix-grow + trim reader as the out-of-band
  path, so `Disc::inputs()` is the single complete source of AACS inputs — one
  reader for every caller.
- **Read-time key-fetch parses `Unit_Key_RO.inf` at the disc's own AACS stride.**
  The on-demand fetch (for a CPS unit not sampled up front) hardcoded the V20
  64-byte stride, so an AACS-1.0 (V10) disc whose key arrived as a VUK derived
  the wrong unit keys. `DiscInputs` now carries the disc's `version`, and the
  context parses at the matching stride — the disc is the single source of truth
  for its own stride (no separate version argument to drift).
- **A dry key-fetch for one unit no longer blocks fetching a different unit.**
  A global "fetch spent" latch meant that once the key service returned nothing
  for one CPS unit's ciphertext, no further unit was ever asked — so a multi-CPS
  disc could strand a unit whose key the service *would* have served. Replaced
  with a per-unit "already-asked-dry" set (still bounded by the fetch budget).
- **`verify::push_ranges` uses saturating arithmetic** so a corrupt-disc LBA near
  `u32::MAX` can't panic (matches `udf::merge_ranges`).
- **Audio no longer corrupts at a stream discontinuity.** At a transport-stream
  discontinuity — a continuity-counter break, an adaptation-field
  discontinuity_indicator, or a concealed-loss gap — the AC-3 / DTS / TrueHD
  parsers held a *truncated* partial access unit and spliced the post-gap bytes
  onto it, manufacturing a corrupt frame (ffmpeg "exponent out of range" /
  "Failed to decode block code(s)" / "Invalid data found") and, for TrueHD, a
  non-monotonic timestamp band on multi-segment titles. The video path already
  resynced via the keyframe gate; the audio parsers now do too — on a
  discontinuity they drop the un-completable partial and resync on the next
  syncword, rebasing the timestamp from the post-gap PES. A discontinuity becomes
  a clean single-frame gap instead of a corrupt splice. Audio has no inter-frame
  references, so dropping the truncated partial is the complete fix; the approach
  matches FFmpeg's parser layer and GStreamer's `tsdemux`.
- **Drive-prep firmware unlock skipped for DVD discs.** An
  `if disc_is_dvd() { return }` guard in `Drive::init()` (present since
  1.0.0-rc.1) bypassed the entire drive-prep unlock step for DVDs. That unlock is
  what removes riplock and raises the drive to maximum read speed — a drive-level,
  disc-independent feature — so every DVD rip ran at riplock speed (~0.4× rated,
  multi-hour ETA). The guard is removed; all disc types now go through the full
  drive-prep sequence. UHD and Blu-ray were unaffected (they already ran through
  the unlock path).

## [1.1.0]

### Added

- **Post-read decrypt-verify gate.** Every AACS unit read off the disc is now
  buffered, re-aligned to its clip-file 6144-byte unit grid, and verified
  (CPI flag → decrypt → strict all-32 TS-sync)
  before it is signed off as good. A unit that no held or freshly-fetched key
  decrypts is treated exactly like a bad read — re-read by
  the patch pass, terminal loss only if truly unrecoverable — closing the
  "silent bad read" class where a sector reads OK but its ciphertext is subtly
  wrong. **Fail-safe:** it only ever downgrades a unit it is *confident* is bad;
  every uncertainty (no keys, a merely-missing key, an unread/zero-filled sector,
  a non-AACS disc) leaves the read byte-for-byte as before. Gated by a
  compile-time kill-switch (`POST_READ_VERIFY`), and container-pluggable (BD/UHD
  transport stream today, with an HD-DVD program-stream seam in place).
- **Every error is now `Error: E<code> <message>`, with an Error Codes
  reference.** User-facing errors show their code so you can look it up, and a
  new **Error Codes** page lists every code with its message, cause, and next
  steps. A contract test guarantees every error variant has a code, a message in
  all seven languages, and a Codes-page entry. Messages are source-agnostic
  ("key source", never a specific database).

### Changed

- **AACS decrypt acceptance is now standards-strict.** A key is accepted only
  when the decrypted unit has the TS sync byte on *all* 32 source packets
  (all-32 TS-sync verify), replacing a majority-vote heuristic where a wrong key
  could coincidentally restore enough syncs to pass and silently corrupt a unit.
- keydb download/save moved out of the library into freemkv-keysources;
  libfreemkv no longer has any keydb I/O (it already held no keys).

### Fixed

- **AACS content-certificate bus-encryption flag read from the wrong bit.** The
  flag is bit 7 of byte 1 (`p[1] >> 7`) but was read as bit 0, so a
  bus-encrypted disc parsed as *not* bus-encrypted — defeating the fail-loud
  guard that refuses to decrypt bus-wrapped data to garbage when no bus key was
  obtained. Also corrected the cc_id offset (byte 14) and the AACS2 type marker
  (`0x10`). Confirmed against real retail content certificates.

- **DVD rips now start on the movie, not the disc menu.** A VTS title VOB's
  start sector was read from the IFO as a VTS-relative pointer but used as an
  absolute disc address, so a DVD title's read extents began `ifo_lba` sectors
  too early — the rip opened on the disc's menu / VMGI region and only drifted
  into the feature minutes later (Silence of the Lambs, for example, showed
  several minutes of the main menu before the movie). The title VOB is now
  rebased to its absolute on-disc location, so the rip begins at the first frame
  of the feature. Aspect ratio and chapter timing were already correct; only the
  starting sector was wrong. (Covered by a new absolute-placement regression
  test.)
- **Container metadata correctness.** Unknown colorimetry now emits the CICP
  "unspecified" code point (2) consistently across the MKV track and the FVI
  sidecar (previously 0); PGS subtitle wipes use the NORMAL composition state
  rather than a full epoch reset; and FVI source-byte offsets are written
  within-sector per the format spec.
- **Multi-extent AACS alignment in `dir://` extraction.** AACS encrypts in
  aligned units of 3 sectors (6 KiB), and the decrypt-on-read gate accepts a read
  only when its LBA is unit-aligned against a base. The `dir://` file-tree
  extractor set that base once, to the file's first extent. A fragmented file
  (Long-AD / continuation-ICB allocation) has later extents starting at arbitrary
  LBAs whose distance from the first extent is generally not a multiple of 3
  sectors, so the first read of every later extent failed the gate, returned a
  decrypt error, and the whole extent was written as a zero-filled hole — even
  though the sectors were readable. The unit base is now re-anchored per extent
  (matching the mux read paths), so each extent gates on its own unit grid.
  Decryption math is unchanged. Same class as the rc.5.2 clip-anchor fix.
- **Distinct "no key" reasons.** When AACS key resolution has usable material
  (device or processing keys) but cannot obtain the disc's Volume ID — needed to
  derive the unit key — freemkv now reports a distinct "AACS Volume ID
  unavailable" error (E7017) instead of collapsing it into the generic "no key"
  error (E7022), which is now reserved for a genuine absence of any key material.
  No key derivation or descramble logic changed — only the reason reported on a
  resolution failure.
- **autorip keydb writes go to the right path.** Auto-download, daily refresh,
  the "Update KEYDB" button, and the startup existence-check now resolve to the
  service's config path (matching where reads look); they previously used the
  CLI's executable-local default.
- **Crash-safety hardening** in `dir://` extraction and keydb writes (fsync of
  files and parent directories around rename).
- **Windows-reserved filenames** (`CON`, `NUL`, `COM1`…) inside a disc's file
  tree are safely renamed on extraction instead of aborting the walk.
- **`--version` now matches the build stamped into MKVs.** The CLI's `--version`
  string and the `MuxingApp` / `WritingApp` fields written into every MKV now
  derive from a single libfreemkv constant — the package version plus the git
  short hash (e.g. `freemkv 1.1.0 (g835cc99)`). The muxer previously kept
  its own copy of that string, so the two could drift; a binary and the files it
  produces can no longer report different versions.
- **DTS-HD Master Audio: a false core-sync inside the lossless extension no
  longer splits an audio frame.** A byte pattern in the extension substream that
  resembled the `0x7FFE8001` core sync word could truncate the lossless
  extension and produce decode errors on the affected frames. The extension
  substream is now sized exactly from its header, so that pattern is skipped as
  data.
- **TrueHD: decode timestamps no longer step backward.** In a case where the
  source PES timing lagged the audio access-unit cadence, the muxed decode
  timestamp could regress (non-monotonic-DTS warnings to the muxer); the running
  timestamp is now clamped so it never goes backward.

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

- **Reverted the rc.5.1 `DefaultDecodedFieldDuration` experiment for interlaced
  SD-DVD.** rc.5.1 added a 20 ms `DefaultDecodedFieldDuration` field element to
  the 576i/480i track header on the theory that Windows derives fps from it.
  Captured evidence showed that element made Windows Explorer report 12.5 fps
  (half) and MediaInfo flip the track to "Frame rate mode: Variable", while
  MakeMKV's rip of the same disc omits it. The element is therefore no longer
  written (`MkvTrack::video` now passes `field_duration_ns == 0`); the track
  keeps `FlagInterlaced=1` + `FieldOrder=TFF` and the full-frame 40 ms
  `DefaultDuration` (`1/DefaultDuration` = 25 fps), matching MakeMKV. How a given
  player or shell handler chooses to display interlaced fps is not guaranteed.
- **Correct AC-3 audio track selected on DVDs with non-standard sub-stream
  ordering.** freemkv assigned each declared audio stream a physical sub-stream
  by ordinal (`0x80+n`), assuming the IFO's first stream lives at `0x80`. On
  discs where the 5.1 main mix sits on a different sub-stream and `0x80` carries
  a 2.0 down-mix (e.g. Silence of the Lambs), the 2.0 was muxed under a "5.1"
  label. freemkv now probes each physical sub-stream's actual channel count from
  the disc — scanning every AC-3 frame and taking the maximum, so a brief 2.0
  logo bed at the feature head can't mask the real 5.1 — and routes each declared
  stream onto the sub-stream that genuinely matches.
- **"Decryption failed" on large AACS Blu-ray titles fixed.** AACS encrypts in
  aligned units of 3 sectors (6 KiB); the unit-alignment gate measured `lba % 3`
  against absolute disc LBA 0, but the unit grid is actually anchored at each
  clip's encrypted-region start. A clip whose start is not 3-sector-aligned had
  its readable units wrongly rejected — failing the feature/large titles of some
  discs while short clips passed. The gate is now clip-anchored.
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
  data via the Stevenson known-plaintext attack and
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
