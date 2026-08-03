# Changelog

## [1.6.0] — 2026-08-03

### Fixed

- Forced-subtitle labels from one vendor's disc metadata were being read as a
  plain flag instead of the multi-value field they actually are, causing some
  full dialogue tracks to be mislabelled "forced" while the real
  forced-narrative tracks were dropped. The field is now decoded correctly, so
  only genuine forced tracks are flagged.
- Content-based forced-subtitle detection previously only sampled the very
  start of a title, where a feature typically has no subtitles yet, so it
  never contributed a verdict. Sampling is now spread across the whole title,
  so a genuine forced track is reliably found.
- A caching bug could apply a partial read's "no forced subtitles here"
  result to an entire disc region, incorrectly suppressing detection on other
  titles that share the same underlying video. Cache entries are now scoped
  to the coverage they actually observed.
- Content-based forced detection no longer promotes a track to "forced" from
  a single flagged subtitle event; it now requires corroborating evidence,
  reducing false positives on discs where only a fraction of a track's
  subtitles are flagged.
- The muxer previously could only add a "forced" flag from vendor metadata,
  never remove one, so a wrongly-labelled track stayed wrong even after
  content analysis proved otherwise. Content evidence can now clear an
  incorrect forced flag as well as set one.
- A generated `.fvi` sidecar index used to report itself as its own source
  file rather than the disc or file it was generated from; it now records
  the real source.
- Fixed several vendor label-parsing bugs across multiple disc authoring
  formats that could apply the wrong language, forced, SDH, or commentary
  flag to a stream — including labels bleeding across titles on discs with
  differing stream layouts, labels merged in with the wrong numbering, one
  parser reading past the end of a title's label list and picking up
  unrelated menu content, and an unrecognized entry silently shifting every
  later label onto the wrong stream. Streams are now bound by identity rather
  than position, and an unlabelled track is preferred over a mislabelled one
  where the correct binding can't be determined.
- Added a missing vocabulary entry for a forced-narrative subtitle marker
  that was previously dropped silently; an unrecognized marker now produces
  one aggregated warning instead of vanishing.
- A key-service outage was previously indistinguishable from "this disc has
  no key" in the reported error. Separate error codes now distinguish an
  unreachable service, a rejected request, and rate limiting from a genuine
  no-key result.

### Breaking

- `Resolution::pixels()` now returns `Option<(u32, u32)>` instead of a bare
  tuple, so "unresolved" can no longer be mistaken for a real 0×0 value.
  Callers now have to decide what an unresolved resolution means for them.
- `DiscSession::into_drive()` now returns a `Result` instead of panicking
  when called on a session whose drive has already been taken.
- Removed the unused `DiscSession::drive()` / `drive_mut()` accessors.
- Removed an internal, unused clip-info parsing path with no callers
  anywhere in the toolchain.
- Added new error codes **E9055**, **E9056**, **E9057** for unresolved MP4
  resolution and sync timeout/worker-loss conditions. Front-ends rendering
  error strings need entries for all three.

### Added

- A new high-level orchestration API (`mux_stream`, `DiscSession`) drives the
  full read → decrypt → demux → write pipeline behind a single call, so
  front-ends no longer need to hand-roll it.
- Per-title stream selection lets a caller prune which audio/subtitle tracks
  get muxed, by track identity, before the mux runs.
- New typed iterators for a title's audio, subtitle, and video streams.
- `MuxOptions` gained a configurable per-call write-pipeline deadline.

### Changed

- The disc-recovery strategy (retry, patch, damage classification) moved out
  of this library into a new `freemkv-engine` crate; libfreemkv now keeps
  only the raw read/decrypt primitives and leaves recovery policy to callers.
- A handful of internal APIs were made public to support the new engine
  crate as a consumer.
- New typed error-classification helpers are now exported at the crate root.

### Fixed

- An undecryptable CSS DVD previously reported success after silently
  skipping every title; a disc-wide decrypt failure is now a hard error
  instead.
- A corrupt `mkv://` input is no longer treated as a title worth silently
  skipping — malformed input now reports as an error rather than an empty
  result.
- AACS 2.1 forensic key resolution now runs once per disc instead of once
  per title, removing a large number of redundant key-service requests and
  disc reads that were previously repeated on every playlist of a
  multi-title disc.
- An internal correctness audit fixed several mux issues: incorrect DTS
  frame drops, TrueHD channel counts understated on AACS discs, a read fault
  confused with a wrong decryption key, multi-title keying edge cases, and a
  user-initiated stop being reported as an error instead of a clean
  cancellation.
- Fixed several UDF filesystem parsing bugs: deleted files and directories
  could still be read as if present (in the worst case breaking enumeration
  of the whole disc volume); the metadata location is now read from its
  authoritative on-disc field instead of assumed; a transient read glitch
  while locating it is no longer mistaken for "not a valid disc"; file
  locations on fragmented files no longer point at unallocated space; and a
  filesystem-structure fallback now retries based on whether data was
  actually found.
- A disc sector known to need re-decryption is no longer decrypted with a
  stale key that produces silently wrong output; it now fails cleanly
  instead.
- An encrypted unit outside every known key range is no longer silently
  counted as successfully extracted.
- Frames dropped during a video resync, and discards from the oversized-frame
  safety net, are now correctly counted and correctly trigger the following
  unit's resync.
- An MP4 video track with no resolvable resolution is now refused rather
  than written as a broken, unplayable track.
- Subtitle/commentary label selection for one vendor format no longer
  depends on unordered internal iteration, which could otherwise produce
  different labels between runs of the same disc.
- A cancelled rip during the final disk sync is no longer reported as a
  hard I/O failure.

### Tests

- The test suite grew to just under 3,000 tests this cycle, and a rare
  intermittent failure caused by a logging-capture race was fixed.

## [1.5.2] — 2026-07-22

### Fixed

- TrueHD 7.1/Atmos channel correction now works on AACS-encrypted Blu-ray/UHD
  discs; it previously silently failed on every such disc and fell back to
  an understated 5.1 channel count.
- AACS 2.1 discs no longer hard-fail when ripping a menu/extras title that
  carries no forensic key segments; such titles now fall back to the disc's
  base key.
- Multi-key AACS extraction now decrypts each clip with its own key instead
  of one key for the whole disc, which previously produced garbage output
  for secondary content.
- A trailing partial encrypted block now fails loudly instead of being
  silently written out as unencrypted-looking garbage.
- CSS-encrypted DVDs no longer mux to garbage: every read path now resolves
  the correct per-title key at read time, an uncrackable title now fails
  loudly instead of passing through scrambled data, and a user-initiated
  stop during key cracking is reported as a clean stop.

### Changed

- DVD scanning no longer cracks a title key up front, since the key is
  per-title rather than per-disc; this also speeds up scanning a CSS DVD
  from about 25 seconds to about 6.
- The DVD entry in the unlock report is renamed from "CSS" to "DVD".

## [1.5.1] — 2026-07-20

### Fixed

- TrueHD audio was being silently dropped entirely (and could send players
  into a memory spiral) due to a checksum bug that made the parser reject
  every audio frame as corrupt. The checksum is fixed; titles ripped while
  this bug was present need a re-rip.
- HD DVD AACS key files are now found regardless of the authoring studio's
  chosen directory/filename convention, instead of only the most common
  layout.
- HD DVD multi-title decryption now reads keys at the correct record size,
  so discs with more than one protected title decrypt all of them instead of
  just the first.
- A disc with marginal, borderline-readable sectors could previously "rip
  clean" while silently containing corrupted data. Such reads are now
  flagged and retried, so a marginal spot either recovers cleanly or is
  reported as an honest gap.

## [1.5.0] — 2026-07-19

### Added

- MP4 can now be used as a source (`mp4://`), for a frame-exact round trip
  into any other output format.
- Native MP4 output (`mp4://`) — rip straight to a play-everywhere MP4 with
  no external transcoder. It's a compatibility export, not an archival
  format: tracks MP4 can't hold (TrueHD, LPCM, bitmap subtitles) are excluded
  with an explicit itemized report rather than silently dropped.
- Five new extraction destinations for pulling one part of a title out on
  its own: video-only, audio-only, and subtitle-only file exports, a
  chapter-markers sidecar, and a full title-structure JSON export.
- Corrupt audio frames are now dropped instead of muxed as decoder-choking
  glitches, across every supported audio format, while keeping audio/video
  in sync.
- Forced subtitles can now be detected directly from subtitle content, not
  just disc metadata, so discs that don't flag them are handled correctly
  too.

### Changed

- The JSON export now includes the complete resolved title model — video,
  audio, and subtitle details, the clip list, and chapter names.

### Fixed

- TrueHD: brief bursts of stream damage no longer discard an entire track or
  shift the audio that follows.
- Free-format MP2/MP3 audio, a legal but less common encoding mode, is no
  longer rejected.

## [1.4.5] — 2026-07-18

### Fixed

- AACS 2.1 forensic discs now mux to a clean single-variant stream instead
  of interleaving foreign forensic data, which previously caused visible
  playback glitches and dropped good frames around each forensic segment.

### Changed

- Types carrying decryption key material now redact their debug output, so
  a key can no longer end up in a log or crash message.
- Hex parsing is now centralized and case-insensitive (previously an
  uppercase-prefixed key value could be silently dropped).
- Internal-only APIs were narrowed in visibility; no behavior change.

## [1.4.4] — 2026-07-17

### Fixed

- Online key lookups were being silently skipped before ever reaching the
  key service, because too few content samples were gathered. The minimum
  sample count now has a compile-time floor so this can't regress.

### Changed

- The set of samples used to build an online key request is now validated
  at construction time rather than by a runtime check that could be
  forgotten.

## [1.4.3] — 2026-07-17

### Changed

- The minimum sample count required for an online key request now has one
  shared definition across crates.
- The online key-service reply is now parsed as a list, supporting both an
  ordinary single key and a full forensic key set.

### Added

- Forensic-disc online key queries now sample from one consistent,
  deterministic segment instead of an arbitrary one.

## [1.4.2] — 2026-07-15

### Fixed

- Fixed a bug where content that decrypted successfully but didn't parse as
  clean video could cause the mux to null out good video and repeatedly
  re-query the key server for a key it already had.

### Changed

- Decryption is now a single, pure operation with no fallback behavior baked
  in; whether decrypted output "looks like" valid video is now a separate,
  caller-decided concern rather than conflated with decrypt success.
- The pass/fail threshold for judging decrypted output as valid was
  tightened.

## [1.4.1] — 2026-07-14

### Fixed

- The mux no longer discards an entire block of good video over a single
  defective packet; a small minority of bad packets in an otherwise-good
  block is now tolerated instead of blanking the whole block.
- 3D Blu-ray (MVC) track signals are now derived from one shared source, so
  they can no longer disagree with each other, and a track is only flagged
  3D when that data is actually available.

## [1.4.0] — 2026-07-13

### Added

- **Blu-ray 3D (MVC) support.** A 3D disc now rips to a single MKV video
  track preserving both eyes, remuxed with no transcoding or side-by-side
  conversion. Verified against a retail 3D Blu-ray disc, with the base (2D)
  view byte-identical to a standard 2D rip.

## [1.3.2] — 2026-07-10

### Added

- Laid groundwork for AACS 2.1 forensic-variant support: the library can now
  identify a disc's forensic variant and classify each block accordingly,
  ahead of full decrypt support landing.

### Fixed

- Corrected a misread field in the AACS 2.1 segment table that had been
  treated as a segment number when it actually identifies the forensic
  variant.

## [1.3.1] — 2026-07-10

### Licensing

- Relicensed to the MIT License from 1.3.1 onward (releases through 1.3.0
  remain AGPL-3.0).

### Added

- HD-DVD title composition now reads authoritative data from the disc's own
  playlist (clips, duration, name, chapters) instead of guessing from clip
  names, with the old heuristic kept as a fallback when no playlist is
  present.

## [1.3.0] — 2026-07-08

### Added

- AACS 2.1 (FMTS) is now recognized and scanned as its own disc format
  rather than misread as plain UHD; the bulk of a 2.1 disc now rips
  successfully, with only the not-yet-supported forensic segments skipped as
  expected loss.
- The AACS 2.1 media-key derivation chain now runs end to end against
  reference data.
- Initial HD-DVD support: HD-DVD is now detected as its own format and its
  video/audio content muxes through the pipeline. Title composition is
  still heuristic — a disc that authors two features under one naming
  convention may present as a single title.
- Program-stream video formats (H.264, VC-1, HEVC on HD-DVD/older discs) now
  get correctly reconstructed per-frame timestamps instead of colliding
  decode timestamps.
- Stream-label detection is now more robust to differing disc authoring,
  with a last-resort fallback that reads menu-artwork languages.
- Loading and saving a key database no longer drops AACS 2.0 host
  credentials on a round trip.

### Changed

- MPEG-2 parsing now shares the same frame-reassembly code as the other
  video codecs, with no change in output.
- Decrypt-failure handling is now unified across encryption schemes rather
  than handled separately per scheme.
- The internal AACS module was reorganized into smaller, focused modules;
  no behavior change.

### Fixed

- Main-title selection now picks the largest title by physical size rather
  than by clip count, so a disc that splits its main feature across many
  small chapter clips is no longer mis-ranked behind a shorter virtual
  composite.
- A fresh-rip ISO write failure at final sync is no longer silently
  swallowed.
- A transient read failure while parsing one clip's info no longer
  suppresses that clip's data for a different title that references it.
- Reverify downgrades that fail to save are now logged instead of silently
  discarded.
- The CLI now sanitizes on-disc metadata (title, labels) before printing
  it, so a malicious disc can't inject terminal control sequences.
- A key-database entry is now validated with the same rule the parser uses,
  so invalid content can no longer be saved as if it were valid.
- autorip now recovers cleanly from a poisoned lock instead of crashing the
  rip thread, and correctly counts resume passes.
- Several smaller fixes: stream numbering, AACS key-source classification,
  discontinuity flagging on a dropped frame, and early-disconnect detection.

### Performance

- Decrypt thread count is now resolved once and cached, instead of being
  recomputed on every call.

## [1.2.2] — 2026-07-04

### Added

- AACS 2.1 Media Key Variant support is now based on the real record types
  found on variant discs, replacing an earlier placeholder that matched no
  real disc.
- Added a single shared function for deriving any AACS key-ladder rung from
  device/processing/media keys, so every consumer uses one hardened
  implementation instead of re-deriving it themselves.

### Fixed

- Fixed AACS device-key fallback derivation, which had been silently broken
  and unusable for both callers that relied on it.
- autorip no longer reports a down key service as "no key found": it now
  probes for a transient outage, retries with backoff, and reports
  "temporarily unavailable" instead of the permanent no-key state.

### Performance

- AACS processing-key resolution on UHD discs is roughly 15× faster,
  dropping from about 37 seconds to about 2.4 seconds.

### autorip

- Move-queue errors in the System tab can now be dismissed individually or
  cleared/refreshed in bulk, without restarting the container.

## [1.2.1] — 2026-07-02

### Fixed

- DVD DTS audio no longer muxes with non-monotonic timestamps, which some
  strict validators rejected. Each frame's duration is now derived from its
  own header instead of sharing one timestamp across multiple frames packed
  into the same container packet. Genuinely corrupt source audio is still
  passed through rather than dropped or fabricated.

## [1.2.0] — 2026-07-01

### Breaking

- The disc's AACS version is now threaded through the key-resolution API as
  an explicit value, since key layout differs by version. This is a
  source-breaking change for external callers of `DiscInputs`,
  `DiscInputsCtx::new`, `read_aacs_inputs`, and `PassProgress`. In-tree
  consumers are already updated.

### Added

- Pass-N marginal-sector recovery gained a roster of specialized recovery
  techniques (read speed, cache bypass, alternate traversal orders) that are
  automatically re-ranked per rip based on which ones are actually working
  on that disc.
- Added an opt-in flat-pool recovery scheduler as an alternative to the
  tiered recovery ladder, useful for discs with heavily hardened residual
  damage.
- Progress reporting now includes a fully-rendered bad-range drilldown
  (chapter, movie-time offset, at-risk time) computed by the library, so a
  client can render the disc map without parsing internal state itself.
- Added a breadth-first "fast capture" patch mode that grabs all readable
  blocks across every bad range in one pass before falling back to slower
  per-sector recovery.
- Mux loss concealment: a block that genuinely can't be decrypted no longer
  passes ciphertext through or produces a broken frame — it's concealed
  cleanly and the codec layer drops forward to the next keyframe, so the
  loss is logged but the output file still decodes cleanly.
- Added a report of which unlock mechanisms (firmware, AACS, CSS) actually
  ran during a given rip.

### Changed

- All hex parsing (keys, IDs) now goes through one shared parser instead of
  several ad-hoc ones.
- AACS sampling and Media Key Block parsing are now more tolerant of
  unusual disc layouts.
- `Disc::inputs()` is now the single source of a disc's AACS inputs,
  replacing several duplicate readers.
- Pass-N recovery was rebuilt as a bounded handler chain — fast jump-ahead
  scanning, then bisection to find exact bad-block boundaries — that can no
  longer hang indefinitely on a wedged drive, with automatic wedge
  detection and recovery.

### Fixed

- DVD DTS/LPCM audio tracks that weren't the disc's first audio stream no
  longer mux silent; stream routing is now based on position rather than an
  incorrect per-codec assumption.
- ISO muxing no longer drops real video at the end of an encrypted content
  fragment; padding at a fragment's tail is now handled separately from
  genuine decrypt failures.
- ISO online key resolution now correctly sends the Media Key Block with
  the request; previously a large-file read limitation left it empty,
  causing every request to be rejected.
- Read-time key fetches now parse the key file at the correct stride for
  the disc's own AACS version, instead of assuming the newer layout.
- A key-service request that returned nothing for one encrypted unit no
  longer blocks fetching a different unit on a multi-key disc.
- Fixed a potential crash from non-saturating arithmetic on a corrupt-disc
  sector address near the numeric limit.
- Audio decoding no longer corrupts across a stream discontinuity (a
  channel change, dropped data, or a concealed gap); the audio parsers now
  resync the same way the video path already did.
- Drive firmware unlock, which raises read speed to normal, was being
  skipped for all DVDs, so every DVD rip ran at a throttled speed. It's now
  applied to every disc type.

## [1.1.0]

### Added

- Added a post-read decrypt-verify gate: every decrypted unit is now
  checked for validity before being accepted, closing a class of "silent
  bad read" where a sector reads fine but its decrypted content is subtly
  wrong. It only ever downgrades a read it's confident is bad — anything
  uncertain is left untouched.
- Every user-facing error now shows its error code, with a new Error Codes
  reference page listing the cause and next steps for each one, in all
  supported languages.

### Changed

- AACS decryption acceptance is now strict (requires all sync markers
  valid) rather than a majority-vote heuristic, which could let a wrong key
  coincidentally pass and silently corrupt a unit.
- Key-database download/save logic moved out of the core library into the
  keysources crate.

### Fixed

- An AACS content-certificate flag was being read from the wrong bit,
  which could defeat a safety check meant to refuse decrypting
  encrypted-bus content with no bus key.
- DVD rips now start on the actual movie instead of the disc's menu
  screens, correcting a title-start offset that was applied incorrectly.
- Several container-metadata correctness fixes: unspecified color info,
  subtitle wipe behavior, and sidecar byte-offset alignment.
- Multi-part (fragmented) files in the directory-extraction path no longer
  have later fragments silently written as zero-filled holes; the alignment
  base is now recalculated per fragment.
- Distinguished "AACS key material present but Volume ID unavailable" from
  a genuine no-key error, so the two are now reported separately.
- autorip's key-database writes now go to the correct configured path in
  every code path (auto-download, refresh, manual update, startup check).
- Hardened crash-safety of directory extraction and key-database writes.
- Windows-reserved filenames in a disc's file tree are now safely renamed
  on extraction instead of aborting.
- `--version` and the app-name fields written into every MKV now always
  agree, since both derive from one shared value.
- A rare false frame-split in DTS-HD Master Audio decoding is fixed.
- TrueHD decode timestamps no longer step backward under certain
  source-timing conditions.

### Tests

- 58 new tests added across the toolchain this cycle.

## [1.0.0-rc.5.3]

### Added

- `dir://` output: write a decrypted file tree straight from a disc or ISO
  instead of a single muxed file.

### Changed

- Key-source error messages no longer assume a local key database is the
  only possible key source.
- The default key-database location is now next to the executable for the
  CLI (the server keeps its own path).
- Simplified command-line flags (dropped a short flag alias and a redundant
  device flag).

### Fixed

- The tool now fails loudly on missing keys or bad input instead of
  silently writing an undecrypted file.

## [1.0.0-rc.5.2]

### Fixed

- Reverted an experimental interlaced-video timing field that had been
  added to try to fix frame-rate display on Windows; testing showed it made
  things worse (some players reported half the actual frame rate), so it's
  removed. The original interlaced flags remain correct.
- Fixed audio-track selection on DVDs with non-standard sub-stream
  ordering, where the main 5.1 mix could be muxed under the label of a
  quieter down-mix track; each stream's actual channel count is now probed
  from the disc rather than assumed from position.
- Fixed a "decryption failed" error on some large AACS Blu-ray titles,
  caused by measuring encryption alignment from the start of the disc
  instead of from each clip's own start.
- The direct disc-to-MKV path now gives a marginal/transient sector its
  full recovery budget before giving up, matching the more thorough
  multi-pass rip path.
- Fixed 4K decode glitches (dropped reference frames) at non-seamless clip
  joins.

### Changed

- The keysources crate is now a pure key lookup; the disc-reading and
  key-validation logic that used to live there moved into the core library.

### Added

- Diagnostic logging (`--log-level 3`) now dumps actual written track
  metadata and the first ~100 frames of a track, to help diagnose
  player-compatibility issues from a log file alone, without needing the
  original disc.

### Verified

- Confirmed, with no code change needed, that DVD opening-frame and
  still-frame handling was already correct, closing out a suspected bug.

## [1.0.0-rc.5.1]

### Fixed

- CSS-protected DVDs on drives that enforce authentication no longer
  produce an empty file or hang; the drive-unlock handshake now runs before
  any data read.
- Keyless CSS title-key recovery now always runs, instead of being skipped
  on certain drive/disc combinations.
- A CSS disc that authenticates but yields no valid title key now fails
  with a clear error instead of writing an empty output file.
- DVD audio channel count is now read from the actual audio bitstream
  rather than disc metadata, so the reported channel count always matches
  what's really there.
- Interlaced video now emits the field-duration metadata Windows uses to
  determine frame rate, fixing incorrect frame-rate reporting on Windows.
- Per-track bitrate tags are now populated so players and file browsers can
  show them without reading the whole file.
- Fixed interlaced field order (was reporting bottom-field-first when the
  source is top-field-first).
- Fixed a DVD bug where a per-title menu screen (e.g. a ratings notice) was
  being prepended to the start of the movie.

### Changed

- The AACS authentication handshake is no longer attempted on DVDs, since
  it never applied to CSS-encrypted media.

### Added

- Structured disc diagnostics available at `--log-level 3`, giving a
  single-command snapshot of disc structure for troubleshooting.
- Reduced routine per-operation log volume.

### Known issues

- Audio track selection can pick the wrong track on discs with
  non-standard substream ordering (e.g. a stereo track instead of the
  intended 5.1); a workaround is documented, and a fix is tracked for the
  next release.

## [1.0.0-rc.4.2]

### Fixed

- Improved Windows file-durability handling: directory sync is now a no-op
  on Windows instead of logging a spurious warning, and file flushes now
  use a read-write handle so they no longer fail on Windows.

## [1.0.0-rc.4] — UNRELEASED

An audit-driven round of correctness, durability, and Windows-transport
fixes. No API changes.

### Fixed

- Partial decryption failures are now correctly counted as loss instead of
  appearing as a perfect rip.
- Key-database and resume-checkpoint writes are now fully durable (atomic
  write + fsync).
- Several error-classification fixes so the reported cause of a failure
  matches what actually happened (connection error vs. parse error, missing
  directory, preserved underlying I/O errors).
- A failed capacity check no longer silently falls back to treating the
  disc as zero-sized.
- An abandoned pipeline can no longer finalize output for a session that
  was already given up on.
- Several Windows SCSI transport fixes (struct layout, field width,
  oversized batch handling, error surfacing).
- A partially-read title now reports accurate loss in its byte count.

### Changed

- Per-read trace logging was demoted to a lower verbosity level so it
  doesn't flood a debug log.

## [1.0.0-rc.2]

Second release candidate for 1.0. Adds keyless DVD/CSS support and correct
DVD video, on top of security and recovery hardening.

### Added

- Keyless DVD/CSS title-key recovery: a CSS-protected DVD now decrypts with
  no key database at all, with a wrong key detected and rejected rather
  than producing silent garbage.
- A proper MPEG-2 frame reassembler fixes corrupted DVD video, with correct
  timestamps reconstructed from the stream.

### Changed

- Video keyframes are now fully self-contained, fixing corruption when a
  source disc doesn't repeat its parameter sets.
- Timestamps now correctly follow presentation order rather than decode
  order, fixing playback of B-frame video.
- Alignment checks are now aware of which encryption scheme is in use, so
  DVD content is no longer incorrectly rejected.
- Output files now record the producing app version for traceability.
- Subtitle display durations are now correctly scaled for non-default
  timecode precision.
- A stop request now interrupts drive-recovery waits immediately instead of
  blocking shutdown.
- Bounded a decompression step against a malformed or oversized download.

### Fixed

- A drive read that returns a successful status but incomplete data is now
  treated as a failed read rather than committing corrupt data.
- Fixed a false transport-error report on Linux for commands that return
  diagnostic data alongside a normal response.
- A capacity value that overflows 32 bits is now rejected instead of
  silently wrapping to zero.

### Security

- Key material is now redacted from all log output.
- Fixed a command-injection risk in the macOS device-access shim.

## [1.0.0-rc.1]

First release candidate for 1.0 — established the full feature set:
multipass sector recovery, content decryption (CSS, AACS 1.0/2.0), disc
parsing, and the threaded mux pipeline.

## Pre-1.0 development

Versions 0.x were the iterative development series leading up to 1.0.
Highlights, condensed:

- **Multipass recovery engine.** An initial full-disc sweep tolerates bad
  sectors, followed by targeted per-sector retry passes; a resume
  checkpoint lets a rip continue after interruption.
- **Drive and SCSI layer.** Cross-platform SCSI transport with full
  sense-code decoding and drive enumeration.
- **Content decryption.** CSS (DVD) and AACS 1.0/2.0 (Blu-ray/UHD)
  decryption from a local key database, with every resolved key verified
  against real disc content before use.
- **Disc parsing.** UDF, Blu-ray playlist, and DVD IFO parsing for title
  and extent assembly, with bounds-checking on untrusted disc-derived data,
  and correct selection of the real feature over a virtual "play-all"
  title.
- **Mux pipeline.** A threaded read/decrypt/demux/codec pipeline taking
  file-backed muxing from roughly 60 MB/s to several hundred MB/s, with
  codec support for HEVC, H.264, VC-1, MPEG-2, TrueHD, DTS(-HD), and PGS.
- **I/O stack.** Bounded disk-cache writeback and batched checkpoint
  persistence keep long sequential rips fast, including over network
  storage.
- **Library hygiene.** No user-facing English text in the library (every
  error is a numeric code), backed by a large spec-grounded test suite.
