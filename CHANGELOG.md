# Changelog

## [1.8.0] — Unreleased

### Breaking

- **AACS keys:** `keys::KeyRing` is the only door to AACS keys. Removed: crate-root `AacsKeyMap`, `DecryptKeys`, `resolve_and_apply`, `KeyFetch`; `resolve_and_apply_traced`, `keysource::{fetch_unit_keys, fetch_fmts_indexes, key_fetch}`, `sector::{KeyFetch, KeyFetchFn}`, `session::{resolve_keys_for, ResolvedKeys}`, `DiscSession::{resolve_keys, key_fetch}`, `Disc::{decrypt_keys, decrypt_keys_for_title, decrypt_with, ensure_decryptable, ensure_decryptable_keys, ensure_title_decryptable}`, `InputOptions::{unit_keys, key_fetch}`, `DiscStream::with_key_map`. `DecryptKeys` is `#[non_exhaustive]`; `AacsKeyMap::from_ranges*`, `DecryptingSectorSource::{with_key_map, set_key_map}` and `aacs::content::{decrypt_unit, decrypt_bus}` are no longer public. Acquire once with `DiscSession::resolve_key_set` / `KeyRing::acquire`.
- **Mux entry point:** `mux_stream` and `MuxInput` are replaced by `mux_with_keys(Source, Option<&KeyRing>, dest_url, opts, ctx)`; a `Source` carries no key material. `InputOptions::keys: Option<KeyRing>` replaces `unit_keys` / `key_fetch`; an AACS image opened without a set is refused before any output. Crate-root `build_iso_pipeline` and `resolve_mux_key_map` are removed.
- **One run context for every stage:** `Ctx { halt, events, stats, diag }` is passed to `input(url, opts, ctx)`, `mux_with_keys(.., ctx)`, `Disc::extract_tree(reader, dest, opts, ctx)`, `write_image(reader, dest, sectors, ctx)` and `PrefetchedSectorSource::new(.., ctx)`. One `Events` trait over one `Event` enum (`BytesRead`, `SectorSkipped`, `UnitBlanked`, `BatchSizeChanged`, `Pass`, `BytesWritten`, `BytesDurable`, `OutputOpened`) replaces `MuxEvents`, `event::{Event, EventKind}`, `progress::Progress`, `EventFn` / `PrefetchedSectorSource::new_with_events`, `DiscStream::{on_event, with_halt}` and `Drive::on_event`; `ExtractOptions::{halt, progress}` are removed (Stop is `ctx.halt`; extract progress is `PassKind::Extract`). `Stats` / `LossReport` count read skips, lost bytes, blanked units and resync drops across the run. `halt::Progress` is renamed `halt::Liveness`. `FREEMKV_SKIP_PARSE` / `FREEMKV_PROFILE` take effect only through `Diag::from_env()` on the caller's `Ctx`.
- **Key acquisition is not bound to a scanned disc:** `ResolvedKeySet` is renamed `KeyRing` and `ResolveKeysOptions` `AcquireOptions` (its `halt` is the run's `Ctx`; `resolve_with_progress` is `AcquireOptions::liveness`). `KeyRing::acquire(&KeyEvidence, &mut Sampler, sources, opts, ctx)` takes what a source can tell (`KeyEvidence`: `MediaId`, AACS structures, `Piece`s in scope, main title, `Detector`), and `KeyRing::acquire_for_disc(disc, reader, scope, sources, opts, ctx)` builds that evidence from a scanned disc (`KeyEvidence::from_disc`). `KeyRing::is_for` takes a `&MediaId` (`Disc::media_id()`), which includes the Volume ID fingerprint. Evidence with no disc hash derives it from the title-key file, so a keydb lookup no longer misses silently. The legacy `aacs::resolve` / `aacs::provider` ladder and `disc::Key` are removed; `KeyOrigin` lives in `aacs::trace` (still re-exported at the crate root).
- **One way to open any input:** `open_source(url, ScanOptions, ctx)` opens every input scheme, `disc://` included (the drive is brought up and scanned; `input("disc://…")` now opens it instead of returning `DiscUrlNotDirect`), and `Source::layout()` is the probe's scanned `Disc` for `disc://`, `iso://` and `dir://`. `MuxSource` is removed: `Source::from_session(&mut DiscSession)`, `Source::from_image(path, ScannedTitle)` (a title scanned elsewhere, never rescanned) and `Source::from_reader(reader, ScannedTitle)` replace `Session`, `Iso` and `Live`, and `mux_url(url, keys, dest, opts, ctx)` replaces `Url`. `MuxOptions` gains `title_index` (it replaces `MuxSource::Session::title_index` and `InputOptions::title_index` for a mux), and `MuxOptions::selection` applies to every source. `scan_iso` / `scan_dir` share the one image probe.
- **Sources and sinks are separate traits:** `pes::Stream` (and the `PesStream` re-export) is replaced by `PesSource` (`read`, `info`, `track_timing`, `codec_private`, `headers_ready`, `config_changes`, `errors`, `lost_bytes`) and `PesSink` (`write`, `finish`, `finish_incomplete`, `info`, `set_track_timing`, `set_codec_private`, `undelivered_streams`). `input()` returns `Box<dyn PesSource>` and `output()` `Box<dyn PesSink>`; a wrong-direction call no longer compiles instead of returning `StreamReadOnly` / `StreamWriteOnly`. `MkvStream`, `StdioStream` and `NetworkStream` implement both (and an inherent `info()`); `CountingStream` wraps a `PesSink`. `SinkCaps::of(&StreamUrl)` (`needs_frames`, `carries_mp2_extensions`) is the one table the mux consults for metadata sinks and MPEG-2 extension tracks.
- **One read loop and one demux for every sector source:** `DiscStream` is removed. A live drive is read by the same prefetch producer and demux as an image, under the drive's Read policy (`sector::read_stage::ReadPolicy::Live`: adaptive batches that shrink on a failing zone and regrow after a clean streak, one ECC recovery read for a unit that still fails, then a zero-filled, counted unit under `skip_errors` or a `DiscRead`); an image keeps fixed batches that stop on the first error (`ReadPolicy::Image`). Live reads now overlap demux and parse on their own thread. Read skips are reported in `MuxOutcome::errors` / `lost_bytes` as before.
- **Stop is the only send bound:** `MuxOptions::send_deadline`, `NO_SEND_DEADLINE` and `halt::POLL_INTERVAL` are removed (use `halt::WAIT_SLICE`); a slow sink blocks the mux until it drains or Stop.
- `Drive::clear_halt`, `Drive::scsi_mut` and `Drive::is_unlocked` and the path-based `platform::fs_type::detect` are removed; use `Drive::attach` / `detach` / `halt` / `token` and `DiscSession::source_mut`.
- **Block sinks:** `io::BlockSink` (`write_at(lba, bytes)`, `finish(Finish::{Complete, Incomplete})`) with `IsoSink` (a sector-exact image) and `NullBlockSink`, opened from a URL by `io::open_block_sink("iso://…" | "null://")`; `write_image` writes through `IsoSink`. `io::null_device()` / `is_null_device()` name the platform's null device (`NUL` on Windows), and `Disc::mapfile_for` keys its private mapfile on it, so a whole-disc copy to `null://` works on Windows.
- **Tree sink:** `io::TreeSink` is the `dir://` output, opened by `io::open_tree_sink("dir://…", force)`: host-safe names, collision refusal, `.partial` then rename, and the AACS/CERTIFICATE directories left out. `Disc::extract_into(reader, &mut TreeSink, keys, ctx)` runs the extraction into it; `Disc::extract_tree` is that chain over a sink it creates.
- **One decryption-stage constructor:** `DecryptingSectorSource::new(inner, keying)` takes `impl Into<sector::Keying>` — `DecryptKeys` (as before), a key ring's view, or the content-detected stage — replacing the crate-internal `detecting` constructor and `with_key_map` / `with_arrival` builders.
- **Removed `io::byte_prefetcher`:** `m2ts://` reads through the one Read stage and `PrefetchedSectorSource` (the same reads, in the same ~1 MiB batches, as before); `BytePrefetcher` and its `PrefetchShell` are gone.
- Error variants no path returns any more are removed with their codes: `DiscUrlNotDirect` (E9009; `disc://` opens through `input()`), `DirImageEncrypted` (E9063; an encrypted folder is keyed like its image) and `MultipassRequiresRaw` (E9082; a multipass recovery may decrypt).
- Unused scaffolding is removed: the `io::sink` module (`SequentialSink`, `RandomAccessSink`, `LocalFileSink`, `SocketSink`, `UdpSocketSink`; `WritebackFile` no longer implements them) and the unfinished `fmp4`, `m2ts_mux` and `HevcMux` muxers.
- `DvdAudioAttr::sub_stream_id` (positional id) is removed.
- `M2tsMeta` gains `timings`, `chapters`, `content_format` and `frame_padding` (all `#[serde(default)]`); a struct-literal build of it must name them.
- `css::CrackOutcome` is `#[non_exhaustive]` and gains `Halted` and `Unreadable(Error)`; it no longer derives `Clone`. `css::crack_key` is deprecated for `crack_key_outcome`.
- `AudioChannels` gains 3.0, 3.1, 4.1, 6.0 and 7.0; `SkipReason` is `#[non_exhaustive]`.
- `scsi::linux::SgIoTransport` `fd` / `fd_recovery` are private; `SptiTransport::reset` (Windows) is removed.
- `freemkv-unlock` is a version dependency resolved by a `[patch.crates-io]` git-tag override; consumers must carry the same patch.

### Output-format changes

- **MKV DefaultDuration of MPEG-2 video** follows the kept frames' measured period when that is another standard rate: a soft-telecined NTSC film title (DVD) declares 23.976 fps instead of the sequence's 29.97, so the frame count agrees with the duration. Video at its sequence rate is unchanged.
- **LPCM** (BD and DVD) is decoded to interleaved big-endian PCM in WAVE_FORMAT_EXTENSIBLE channel order (BD pad channel removed, LFE/surround remapped, DVD 20/24-bit groups unpacked) at the source depth: 16-bit stays 16-bit, 20- and 24-bit sources output 24-bit; MKV `BitDepth` matches, and M2TS/MPG output keeps the depth. BD 20-bit packets are kept (24-bit container); reserved header codes are dropped. On M2TS output LPCM is repacked to the BD layout; LPCM BD cannot carry (e.g. 44.1 kHz) is dropped and reported.
- **M2TS:** reordered video (H.264, HEVC, MPEG-2, VC-1) now carries a DTS (H.222.0 2.7.5); PTS round to the nearest 90 kHz tick (a PTS can be 1 tick later than 1.7.7), the first frame lands one second above zero, PCR never decreases with B-frames; output always declares `bd-ts` in its FMKV header. PGS `.sup` ticks use the same rounding.
- **MKV:** DTS, DTS-HD HRA and DTS-HD MA are all `A_DTS`; streams with no Matroska CodecID (text subtitles, unknown codecs) are omitted rather than declared under another codec; SDR BT.2020 video is tagged with the BT.2020 transfer (14), not PQ; the timeline origin is the earliest sample of any selected track (audio and subtitles before the first video keyframe are kept at their true offsets; video starts at its first keyframe), and Blu-ray clip IN applies only to BD-TS sources, whose frames before the IN are dropped and counted; VobSub cues without a stop command end at the next subtitle (10 s cap) instead of 30 s. **MP4:** a track that starts after the earliest sample keeps its offset through an empty edit, and video before its first keyframe is not stored.
- **One decryption stage for every input:** `mpg://`, `m2ts://`, `mkv://`, `mp4://`, `network://` and `stdio://` pass the same content-detected stage as disc/`iso://`/`dir://`. CSS packs are cracked and descrambled, AACS units are decrypted with `InputOptions.keys`, and clear containers pass untouched. An encrypted loose `.m2ts` is decrypted (keys proven on arrival) or refused with E7022 before any output instead of being muxed as ciphertext; `--raw` passes ciphertext. A clear `.m2ts` whose CPI bits were left set still opens, also when a unit has damaged packets (half of them synced suffices). Encrypted content under a clear container's scheme is refused (E7023/E7022). A file whose start is zero-filled is judged from its first written sector; a file judged clear still refuses a scrambled pack (E7023). On `m2ts://` an unflagged unit with no TS structure is blanked and counted in `errors()`.
- **One CSS pack test for disc and file:** `css::is_scrambled_pack` (so the crack scan, `descramble_sector` and `descramble_region`) reads the scramble bits from the pack's first PES header: past pack stuffing and any system header, map, padding or nav packet, and only from MPEG-2 PES flags. Every DVD-Video pack is judged as before; a stuffed, map-first or MPEG-1-PES pack whose byte 0x14 merely looks flagged is no longer descrambled on the disc path, and a stuffed scrambled pack is descrambled by its own flags.
- **A flagged partial AACS unit that ends the source** (a truncated copy) is blanked and counted as damage instead of failing the read.
- **Decrypted BD-TS units have the Copy_permission_indicator cleared** (top 2 bits of each TP_extra_header), so decrypted output differs from 1.7.7 in those bits. HD DVD and units left as ciphertext are untouched.
- **`json://`** audio `sample_rate` is a number in Hz (`null` when unknown), not a string such as `"48kHz"`; a `sample_rates` array lists every rate a stream carries.
- **FMKV (`network://`, `stdio://`) header version 2** is used when a track carries decoder timing (Opus CodecDelay/SeekPreRoll) and adds per-frame DiscardPadding; streams without timing stay version 1. Headers now also carry display aspect, CICP, stream purpose/qualifier, chapters and content format, and PIDs actually written by M2TS. A header declaring more than 256 streams is refused with E9008. An older receiver omits the MKV Channels element for the new channel-layout strings; keep both ends on one version.
- **`dir://` image bytes:** the PVD declares interchange level 3, Unique IDs start at 16 and the volume id round-trips through the d-string encoder; `dir://` sources are stamped as images in the `fvi://` provenance header.
- **Output title names:** a title read from a drive (`disc://`, a server live rip) is named by the disc's meta title (else its volume id) in every container's title field and in `fvi://` provenance; one read from an image or folder (`iso://`, `dir://`, a staged image) by the disc's meta title, else its own playlist name as in 1.7.7, so an image and its extracted folder name a title alike. 1.7.7 named server-live rips by the MPLS playlist file. `fvi://` provenance of an `iso://`/`dir://` source now carries the volume id.
- **TrueHD 7.1 / Atmos labels on every path:** the playlist labels a 7.1 or Atmos TrueHD track 5.1 at 48 kHz; every mux (live disc, staged image, `iso://`/`dir://`, containers) now completes the track's channels, sample rate and label from its first major sync before the output opens, and `json://` / `chapters://` get the completed title too. 1.7.7 corrected only `iso://`/`dir://` URL rips, so live and staged-image rips now label such tracks 7.1 / Atmos.
- **Track selection applies to container inputs:** `selection` keeps a subset of the tracks of an `mkv://`, `mp4://`, `m2ts://`, `network://` or `stdio://` input (renumbered in order, video always kept), as it already did for disc titles; an unknown PID is refused at open.
- **A live DVD extracted to `dir://` is descrambled:** `Disc::extract_tree` finds and cracks each scrambled VTS from its content on every DVD-Video layout. A live drive scan records no disc-wide CSS key, so a live DVD's title VOBs were written still scrambled.
- **An encrypted disc folder is an ordinary `dir://` source:** a folder whose sampled content units are AACS-encrypted keeps its AACS verdict and is keyed and decrypted like an image of the same disc, instead of being refused with E9063; with no key it is refused before any output with E7022.
- **Frames dropped by the resync gate count as errors** in `MuxOutcome::errors` / `lost_bytes`, together with AACS units blanked as damaged.
- **A failed or stopped `network://` / `stdio://` sender resets the connection** (`Stream::finish_incomplete`), so the receiver errors instead of finishing a truncated title.
- **Key-service 404 / 422 answers are E7022** (no key opens the title), and a key-service outage or unreachable source is a source failure, never "no key for this disc".
- Diagnostics log tags: `tag=clip` is now `tag=bd.clip`; `tag=aacs` no longer logs key_source/vuk/unit_keys. `fvi://` and FMKV video `dar` for SD without a declared aspect is `0:1` (unknown), not 720:576. `Error::ImageTruncated` displays as `E<code>: have/want`.
- DVD audio and subtitles are routed by the title's PGC AST_CTL / SPST_CTL; track order, language and set may differ from 1.7.7 on discs whose physical ids are not ordinal. The mux-time AC-3 channel remap is gone.

### Added

- `mpg://` as a source and a destination: a DVD-Video / MPEG-2 program stream file reads through the decryption stage and the DVD sector pipeline (a scrambled `.vob` is cracked once, `--raw` keeps the ciphertext; a clear MPEG-1 or stuffed MPEG-2 stream is no longer read as scrambled, and a scrambled pack with no key fails E7023). `MpgSink` writes an H.222.0 program stream with P-STD pack timing, system header and PSM; `Error::MpgNoVideoTrack` (E9074), `Error::MpgUnpacketized` (E9075). `FileSectorSource::open_padded` zero-pads a partial tail sector.
- `keys` module: `KeyRing::acquire(evidence, sampler, sources, opts, ctx)` (`KeyScope::{None, Titles, WholeDisc}`, `AcquireOptions`, `KeyResolution`) asks the key sources once before any output and proves each held key against the disc's ciphertext per stream file; keys and the Volume ID stay in memory (`Debug` redacts). Also `title_reader`, `whole_disc_reader`, `decrypt_status`, `check_decryptable`, `KeySetStatus`, `ForensicState`, `DecryptStatus`, `ProofCache`. A piece that cannot be proven up front is proven on first read; a unit no held key opens stops a title rip with E7022; in an image or folder it is blanked, warned and counted. `ResolveCtx::halt()` / `progress()` give sources the Stop token; key-service retries back off 1 s to 8 s. `MediaKeyVariantError::code()` returns E7100-E7108.
- `disc_root_of(path)`: the AACS disc folder a loose Blu-ray clip sits in (`<root>/BDMV/STREAM/x.m2ts` with `<root>/AACS`), so a caller can resolve the clip's keys on `dir://<root>`.
- `mux::fit_report` / `FitReport` / `SkipReason` (`Mp2Extension`, `NoStreamId`, `UnmappableSubtitle`): a container-neutral pre-mux plan of which streams a destination carries. `mp4_fit_report` is unchanged.
- `DiscSession::{open_with, scan_with, finish, attach_progress, token, from_drive, source_mut}` and `session::Finish::{Release, Unlock, Eject}`: a session runs under the caller's Stop token and ends on the handle it holds. `Drive::{attach, detach, attach_progress, halt, token}`: every CDB is checked against the op token.
- `halt`: waits re-check every `WAIT_SLICE` (20 ms); `Liveness`, `StallTimer`, `join_within`, `spawn_drive_holder`. `halt` is available with only the `scsi` feature.
- `NetworkStream::listen_with_halt` / `accept_from_with_halt`; `Event::BytesDurable { bytes, total }` while the output flushes; `io::{ArtifactLock, durable_sync_file, FlushProgress}` (`<final>.lock` sidecar; E9073 `TimedOut` after 30 s with no change); `Pipeline::{set_op_token, progress}`, `Sink::close_stopped` (a consumer closing after Stop keeps `*.partial`); `ISO_MUX_BATCH_SECTORS` is public.
- MPEG-2 multichannel extension audio (DVD `0xD0|n`) is a dependent track (`AudioStream::is_mp2_extension`) carried in the IR and over FMKV; sinks with no mapping list it in `MuxOutcome::undelivered_streams`, `mp4://` skips it (`Mp4SkipReason::Mp2Extension`). MPEG-1/2 audio (PES `0xC0|n`) is demuxed on DVD.
- Errors: E9085 `MuxBatchSectorsZero`, E6020 `ImageEndsBeforeRead`, E6021 `BusStreamUnmapped`, E6022 `ImageScoped`, E6023 XPL too large, E7031 `AacsKeyFileUnreadable`, E7032 `WholeDiscKeyMissing`, E7033 `AacsNoUsableHostCert`, E7034 `AacsVidNeedsDisc`, E7100-E7108 MKB variant faults, E9071 `StreamClosed`, E9072 `StreamHeaderWritten`, E9073 `TimedOut { op }`, E9074, E9075, and the remux/preflight set E9077 `RemuxVerifyFailed { kind, path }` (`E9077: <kind> <path>`, or `runtime-mismatch <have>/<want>`), E9078 `MuxIncomplete`, E9079 `RemuxStagingInvalid`, E9080 `StagedCopySizeMismatch`, E9081 `WorkerLost { op }`, E9083 `StreamLanguageUnknown`, E9084 `RemuxTargetExists`.
- `whole_disc::{whole_disc_reader, WholeDiscReader}`: the decrypting reader for whole-disc / image to ISO copies; it keys every stream file no title plays (probed at its first unit and up to 32 across it) and blanks, warns and counts the units of a stream file no held key opens (E7032 only when no file opens at all, a source faults, or no key is held). `ScanOptions::raw_copy` is kept but has no effect: an unreadable `Unit_Key_RO.inf` never fails the scan.
- `sector::bus_removal::{UnmappedStreamFile, ensure_image_debussable}`, `BusMap::unmapped`, `SectorSource::unmapped_stream_files`, `Disc::{mkv_staging_ranges, stream_content_ranges, inputs_with_samples}`, `DecryptingSectorSource::{blanked_units, clear_unit_base}`, `DiscPresence` / `disc_presence(path)`, `Playlist::duration_ticks`.
- `probe_mkv` / `probe_mkv_with_cues` read a Matroska file's Info and Tracks (and last Cues time) without reading a cluster; `parse_freemkv_version`.
- `mkv://` read-back carries display shape (aspect, PixelCrop), colour, frame rate, `LanguageBCP47` and chapters and undoes content encodings (header stripping, zlib); `mkv://` writes `MaxBlockAdditionID` for MVC 3D. `Mp4Reader` reports colour (CICP, HDR10, HLG, Dolby Vision), frame rate, duration, QuickTime v1/v2 and ISO v1 audio entries, `dac3`/`dec3` layouts and DTS `ddts` rate.
- M2TS output accepts DVD (MPEG-PS) sources: PIDs BD-TS cannot carry are remapped into BD PID ranges (a track with no free PID is omitted with a warning).
- `mux::codec::ns_to_ticks`, `CodecParser::config_changes()` / `Stream::config_changes`, `Stream::set_codec_private`. DTS Express (EXSS-only) streams are framed; MPEG-1 video is framed per picture.
- `libfreemkv::spec` (quoted spec text with registry check); `test-util` feature: `test_util` disc/image fixtures and `FakeTransport` for consumers' tests. Labels: large BD-J jars read in place, DRA CodingTypes mapped.

### Changed

- **AACS damage policy (E7013):** a flagged AACS unit no key opens (no TS sync, zero-filled or garbled head, lone FMTS verify failure, off-grid) is zero-filled, warned and counted on every path (ISO mux, live mux, extract, whole-disc reader) instead of failing with E7013, with an end-of-rip "N damaged AACS units blanked" log. A wrong key still fails: E7013 at resolve (or when two or more FMTS units fail under one key with none verifying), E7022 on arrival in a title rip (an image or folder blanks the unit instead).
- A BD-TS mux with no AACS key set fails E7022 on the first AACS-flagged unit instead of writing ciphertext; an AACS HD DVD session with no set fails `NoDiscKey`. A key-set stop (E7022) is never shrunk, recovered or skipped.
- A Stop during a mux is always `completed = false, halted = true` (`MuxOutcome::halted`), whether it lands during the open or the pump: `mux_with_keys` no longer returns `Err(Halted)` from a stopped open. A Stop reaches `iso://` and `dir://` sources inside a blocked read, the image scan and the CSS crack (it was seen only between frames), and `iso://`, `dir://` and `mpg://` URL muxes report read progress like the disc arms. A stopped read is a stop, not a skipped sector. A read error repeats on every later `read()` of the highway stream. A halted `PrefetchedSectorSource` close is `Err(Halted)`, not EOF.
- `Disc::scan` follows the standard AACS order (UDF, `Unit_Key_RO.inf`, certificate, MKB by plain reads, then the handshake, only on a disc with an AACS directory), no longer reads the MKB from the drive, and passes `None` to `KeySource::host_certs`. A live AACS disc whose `Unit_Key_RO.inf` is missing or unreadable is warned and scanned on, with E7031 recorded in `Disc::aacs_error` and every key refused (titles that need keys then fail E7022 on the normal key path); image and folder scans record it the same way. A handshake transport fault fails with `Drive::init`'s error and a Stop returns `Halted`. `DiscSession::scan` returns the key sources on failure so a retry keeps them.
- Host-key (cert-route) bus-encrypted discs: every `/BDMV/STREAM` file is de-bussed (BEE flag and per-unit CPI honoured; CPI 0 passes through). A stream file whose extents cannot be read is re-read with FUA (bounded, 5 s pause a Stop ends), then recorded; `ensure_image_debussable` refuses a whole-disc image with E6021 naming each file and cause, `extract_tree` counts such files lost whole, and `stream_content_ranges` fails E6021.
- `DecryptingSectorSource` has no default AACS unit base: a content read before `set_unit_base` fails `DecryptFailed`, as does a flagged BD-TS unit lacking TS sync at byte 4.
- `read_mkb_from_drive` returns `AacsKeyRead` for a short, over-long or header-only pack; record 0x07 is no longer read as a cvalue table; a content certificate type other than 0x00 / 0x10 no longer parses. AACS 2.1 tries every device key before declaring the MKB underivable; an HD DVD VTKF is parsed by its magic. A CSS crack that read no sector is `Unreadable`, never `Unencrypted`.
- Key-source failures return `Err` (never an empty key set); a read-time answer that opens no sample is skipped, and the key pool and per-answer keys are capped.
- `drive_has_disc` (Linux, Windows) classifies TEST UNIT READY sense: `Ok(false)` only for NOT READY 3Ah and 30/03, 30/07 (cleaning cartridge); 04/01 and other NOT READY states are `Ok(true)`; UNIT ATTENTION is re-polled up to 4 times. `Drive::wait_ready` rides out transport failures (5 s), sends one START UNIT on 04/02, fails at once on 30h, and returns 3Ah after 10 consecutive MEDIUM NOT PRESENT answers.
- `list_drives` on Linux without sysfs keeps an sg node unless INQUIRY says not optical, or open fails with ENOENT/ENXIO/ENODEV/EACCES/EPERM. Dropping a `Drive` or transport unlocks the tray only if it locked it. `DriveId::from_drive` reads firmware date and serial from GET CONFIGURATION features and returns `Halted` on Stop.
- macOS: opening a drive is cancellable, each CDB uses the caller's timeout (`timeout_ms` 0 is 60 s), and `find_drive` releases an empty drive before the next candidate. The shim is built against `MACOSX_DEPLOYMENT_TARGET` (11.0 aarch64, 10.12 x86_64).
- `Pipeline::finish_with_halt` waits on consumer progress (the 600 s join is a stall window). `sync_all` on a `WritebackFile` fails E9056 only after 60 s without flush progress and returns `Halted` on cancel; `WritebackFile` flushes in chunks (64 MiB, down to 4 MiB). Linux `sync_file_range` errors are latched; `write_image` fsyncs the parent directory; reads past EOF of an image return `ImageTruncated`.
- `network://` send and receive accept LAN targets: loopback, private (RFC 1918), link-local, ULA and CGNAT addresses connect. Only unspecified, multicast, broadcast and reserved addresses are refused (E9022); a caller that takes targets from untrusted input must vet them itself.
- `network://`: send connects to each vetted address in turn (10 s timeout); both ends enable TCP keepalive (a rejected option only warns); receive no longer blocks on a vanished sender or a Stop. `stdio://` input fails E9008 on a non-FMKV or damaged header once bytes were consumed.
- Container headers (MKV, fragmented MP4, M2TS, network) wait (5 s of source PTS, 2048 frames or half the header buffer) for the AAC AudioSpecificConfig and BD LPCM layout byte; a later config is back-patched into a seekable MKV. `Event::OutputOpened` lists only the streams the sink writes. A mux with `batch_sectors == 0` fails E9085.
- DVD: titles map through `VTS_PTT_SRPT` (TTN 0 rejected), one angle counted, an unusable IFO falls back to its BUP; MP2 `Channels` follows CRC-verified Layer II frames; the live path shares the file-backed path's access-unit assembly and H.264 second-field merge; unmapped PS streams warn once.
- Codec framing: ADTS and MPEG audio resync only on a header confirmed by the next, so a few KB of garbage no longer poisons a track; MPEG-2 field order follows picture_structure; H.264 1080i I/P field pairs are keyframes; HEVC arms CRA-to-BLA on gaps. TS demux skips duplicate packets, resyncs after a byte slip, honours transport_error_indicator.
- `mp4://` DTS follows ETSI TS 102 114 Annex E (`ddts` carries the stream's maximum rate; 96 kHz DTS-HD MA reads back as 96 kHz); video CTS is rounded to the nearest tick; `Mp4Reader` labels MPEG-audio `mp4a` as Mp3/Mp2.
- UDF: a corrupt directory FID is warned and stops that directory listing (the root keeps the entries before it, a subdirectory lists empty); AD chains are validated (`UdfAdChainTooLong`); fragmented Metadata Files are mapped. Directory images report `DirImagePlacement` / `DirImageFanout`. MPLS: a truncated play-item list keeps the items before the damage (warned; `MplsParse` only when none remain); a CLPI cut short of its 60-byte head still parses with an unknown (0) packet count, sized from the clip's extents. bdnav / dvdnav VM follows libbluray / libdvdnav.
- Labels: BD-J detection reads each jar once under a shared inflate budget and abstains on unreadable evidence; `clpi_audit` keys streams by (clip, PID).
- HD DVD: a title naming an absent or truncated clip is dropped; the `X!` AACS directory gates identify and scan and is stripped from extractions; oversized XPL is E6023.
- `Error::DirNameCollision` and `StreamLanguageUnknown` display with control characters escaped.

### Fixed

- `m2ts://` output of a multi-clip title rides the clip-join timeline like the MKV, MP4 and MPG sinks: the PTS no longer restarts at each join, and reordered video keeps its DTS after the first join instead of falling back to PTS only.
- An image title whose extent ends off the 3-sector AACS unit grid (an HD DVD `.EVO`) no longer fails E9030 at that extent's end: the sub-unit tail is read, passed when clear and refused (E7013) when it is part of an encrypted unit.
- Every AACS format takes one encryption decision in `KeyRing::acquire`: content its own detector reads in the clear needs no key and asks no source, whatever key files the disc declares (an HD DVD copy in the clear declaring several keys, and an FMTS disc whose forensic segments are also clear, no longer ask for or refuse on keys); encrypted content is keyed by the format's rules and refused (E7022 / E7032) when no source has the key.
- A zero read batch (`MuxOptions::default()` carries `batch_sectors` 0) is refused with `Error::MuxBatchSectorsZero` (E9085) on every path (live, session, ISO, URL) instead of spinning until Stop.
- A stopped or failed read can no longer finalise a truncated MKV / M2TS as a complete title; `extract` after a drive Stop leaves the file `.partial` instead of finalising zeros.
- DVDs with MPEG audio (common on PAL) ripped an empty track; anamorphic DVDs got wrong or missing subtitle languages; a partial angle-cell filter dropped cells.
- Blu-ray picture-in-picture PG entries no longer appear in the subtitle list; HD DVD triple-layer layer count and combo audio channel counts are correct; HD DVD XPL timecodes at 60fps timeBase run at 60000/1001.
- TrueHD channel labels keep the LFE split (3.0 was "2.1", 7.0 "6.1").
- A cleaning cartridge reads as no disc, so autorip no longer rips it; a drive becoming ready (04/01) is no longer "no disc".
- `mkv://` writer fails every later write and `finish()` after a frame write error and no longer panics; read-back counts malformed blocks and undecodable frames as lost data, rejects duplicate TrackNumbers and over 512 TrackEntries (`MkvSourceInvalid`), and treats chapters, tags and attachments as best-effort. PCM tracks without BitDepth use bounded read-ahead and a multi-block estimate.
- `Mp4Sink::write` after `finish()` fails E9071; `Mp4Reader` truncated 64-bit box size is E9049 and QuickTime v1 `esds` is found; `mp4://` no longer invents 48 kHz for reserved AC-3 / E-AC-3 / DTS rate codes.
- PGS clear events, durations over 30 s and gap handling; DVD-sub orphan continuations; chapter names escaped in WebVTT; TrueHD, DTS EXSS, MPEG audio free-format and `read_ue` edge cases; a first PES starting mid-frame drops the leading fragment.
- UDF stale cache window after a failed read; Stop is honoured in image scan, the FE re-read pause and READ CAPACITY retries; a `/dev/null` mapfile uses a private per-process temp dir; `extract` sanitises reserved file names and checks extent LBA arithmetic.
- `GET CONFIGURATION` honours Data/Additional Length and feature code; MODE SELECT list length is MDL+2; Windows SPTI timeout/alignment/bounce-buffer fixes; macOS DiskArbitration claim leak.
- Labels: Paramount "feature" matching, Criterion duplicate mappings, B/T language match, bdmt setNumber. Diagnostic opening capture no longer aliases track index 256 and above.
- `m2ts://` output now carries a PAT and an HDMV-registered PMT (PID 0x0100, repeated every 100 ms) declaring each track's stream_type, so FFmpeg and players identify every track: BD LPCM (0x80) decodes as `pcm_bluray` at the source depth instead of "unknown"/`mp3`. AAC is re-framed as ADTS from its AudioSpecificConfig (stream_type 0x0F; a config ADTS cannot signal is dropped and reported), MP2/MP3 are 0x03, and both use the MPEG-audio PES stream_id 0xC0.
- `m2ts://` output now carries a program clock: PCR packets on PID 0x1001 (the PMT's PCR_PID, BD-ROM convention) at most 100 ms apart, and every packet's TP_extra_header arrival time stamp on the same 27 MHz clock, at most 1 s ahead of its PES's DTS (it was 0). A timeline jump of more than 10 s restarts the clock with the discontinuity_indicator; PID 0x1001 is never given to a stream.
- `m2ts://` output keeps every raw_data_block of an AAC access unit taken from a multi-block ADTS frame: the ADTS header declares the block count (from the frame's duration) instead of one, which made decoders play only the first 1024 samples.
- Loose `m2ts://` input keeps AAC, MP2 and MP3 that other m2ts muxers declare as PES private data (stream_type 0x06): an MPEG-audio stream_id plus the ES sync names the codec. `mp4://` carries AAC, MP2 and MP3 (`mp4a` + `esds`) instead of skipping them.

## [1.7.7] — 2026-09-26

### Maintenance

- Replace comment-overflow documentation with concise source contracts and README instructions; enforce the shared comment policy in CI.

### Fixed

- Preserve PGS clear events so forced subtitles disappear and FFmpeg remuxing retains valid timestamps (#52).
- Reassemble AAC/MPEG audio frames, recognize standard transport audio types, retain opening audio and preserve MKV decoder delay/padding.

### Added

- FFmpeg interoperability validation on QA, using generated fixtures and decoded-content/timing comparisons.

## [1.7.6] — 2026-09-26

### Changed

- Version aligned to 1.7.6 for the unified release. No functional changes to this crate; the release is driven by the freemkv 1.7.6 Linux desktop shell (GTK4 + libadwaita) and the rip-finished desktop notification (issue #56).

## [1.7.5] — 2026-09-23

### Changed

- Version aligned to 1.7.5 for the unified release. No functional changes to this crate; the release is driven by freemkv-unlock mirroring the freemkv-firmware 0.9.0 ABI (the drive's `Ake` and `Bus` levers retired into a single `Encryption` lever).

## [1.7.4] — 2026-09-21
### Fixed

- BD-J / Java-menu main-title selection reworked (#45): a chapter-completeness failsafe, a BD-J menu-walk (parses the BDJO objects and jar manifests), and a stream-richness tiebreak now cooperate so discs driven by a Java menu pick the real main feature — keeping the Dolby Vision enhancement layer and all subtitle tracks — instead of a lower-id sibling playlist.
- PGS subtitles now get a synthesized `BlockDuration` on every block, fixing the "Timestamps are unset" mux error, plus a defensive HEVC NAL length-prefix guard (#52).
- keydb "no entry" is de-conflated into a true miss versus matched-but-no-usable-VID, and the disc hash is surfaced; adds `KeyStep.matched_entry` and `KeyStep.store_entries` (#46).

### Maintenance

- CI moved to the central reusable workflows.

## [1.7.3] — 2026-09-19

### Fixed

- MPLS title enumeration: the STN (stream number) table of the first PlayItem is now located past the multi-angle *angle block* instead of at the fixed offset 32. A multi-angle first PlayItem (e.g. a seamless-branch UHD title) carries `number_of_angles` extra 10-byte angle entries before the STN table, so the old fixed offset read the wrong bytes and misparsed the primary title's streams — the root cause behind autorip/CLI picking the wrong (or no) main feature on some UHD discs. Validated against a 92-disc real-media corpus with zero regressions (issue #45).

### Added

- `disc::read_structure_files()` — captures a disc's **structure metadata** (`BDMV/index.bdmv`, `MovieObject.bdmv`, `PLAYLIST/*.mpls`, `CLIPINF/*.clpi`, `BDJO/*.bdjo`, `META/DL/*.xml`, DVD `VIDEO_TS/*.IFO`) over any `SectorSource` (drive, ISO, `dir://`). No audio/video essence and no AACS keys are read, so the result is safe to attach to a bug report (a few hundred KB). Powers `freemkv info … --share` so a reporter can reproduce a title-selection issue without shipping the full ISO.

## [1.7.2] — UNRELEASED

### Fixed

- ISO imaging: the READ CAPACITY (disc-size) query is now retried, with a UDF partition-size fallback, and `image_read_sectors()` hard-errors (`EmptyImage`) when the size is unavailable instead of writing a silent 0-byte ISO reported as success. Fixes an intermittent empty ISO caused by a transient capacity-query transport failure (shared CLI/autorip engine).

### Changed

- Unified release with freemkv-unlock 1.7.2 (firmware ABI v2).

## [1.7.1] — 2026-09-14

### Changed

- Unified release with freemkv-unlock 1.7.1 (LibreDrive profile-match fix). No functional changes to this crate.

## [1.7.0] — 2026-09-02

### Fixed

- `mux/mp4`: the capped child-box scan treated any `size < 8` as a stop, which dropped a 64-bit `largesize` child box (`size == 1`, real length in the following 8 bytes) and every sibling after it; a `size == 0` (run-to-end) box was mishandled too. Both are now honoured, so an MP4/MOV whose boxes use 64-bit sizes is walked fully instead of truncated.

- Linux: `SgIoTransport::raw_command` clamped an over-length CDB with `cdb.len().min(16)` instead of routing it through the shared `checked_cdb_len` guard that `execute()` uses. Under SPC-4 a CDB's length is fixed by its opcode, so truncating one does not shorten the command — it sends a descriptor the drive will read as something else. An empty CDB was likewise unguarded, and went to the sg driver as a zero-length command descriptor. Unreachable — `raw_command` is private and its one caller passes a fixed 6-byte ALLOW MEDIUM REMOVAL — but it was the one transport path that did the thing the guard exists to prevent.
- Linux: the fd hand-off between a background recovery thread and `SgIoTransport::drop` had no release edge, so a recovery thread that published its fd after teardown had drained the slot was not guaranteed to observe `dead == true` and could leave the descriptor unclosed. `Drop`'s claiming `swap` was `Acquire` (a relaxed store, heading no release sequence) and the recovery thread's `compare_exchange` was `Release` (a relaxed load, acquiring nothing) — neither side alone was enough, and upgrading only one still leaks. Both are now `AcqRel`. Worst case was a leaked file descriptor on a transport already being torn down.

- Windows: `scsi::windows::list_drives` reached into the `rip`-gated `drive`/`identity` modules, so a scsi-only consumer (`default-features = false, features = ["scsi"]`) failed to compile — `--all-features` CI never exercised that combination. It now enumerates directly via `scsi::open` + `scsi::inquiry`, like the Linux and macOS backends. Surfaced by freemkv-firmware, which links libfreemkv scsi-only.

### Changed

- The Linux recovery-thread fd hand-off moved to `scsi::fd_handoff`, where it is compiled and unit-tested on every platform (`linux.rs` is built on one host only) and model-checked under `loom`. No public API change.

### Security

- Fixed a soundness bug (GHSA-j8ww-f5fg-9pmh, low severity): `PrefetchedSectorSource::into_channels` handed out a `Sender<Vec<u8>>` recycle channel whose buffers the producer re-exposed with `unsafe { Vec::set_len }` guarded only by capacity, so a downstream caller recycling a `Vec::with_capacity(n)` (len 0) could drive `set_len` over uninitialized memory — undefined behaviour reachable from safe code. The producer now uses `Vec::resize`, removing the `unsafe`; buffer pooling (the real cross-thread alloc/free win) is unchanged. No in-tree caller triggered it.

## [1.6.14] — 2026-08-31

### Changed

- Version aligned to 1.6.14 for the unified release.

## [1.6.13] — 2026-08-28

### Added

- Capture a few additional drive buffers during `info --share` collection on supported drives.

## [1.6.12] — 2026-08-27

### Fixed

- macOS: hold a DiskArbitration claim (plus a mount-approval dissenter) for the SCSI transport's lifetime so `diskarbitrationd` can't remount the disc mid-rip — the cause of the E1000 "disc stolen mid-rip" failure on macOS.

### Changed

- Comment and documentation cleanup.

## [1.6.11] — 2026-08-26

### Added

- Navigation-driven main-feature selection: selection now follows the disc's
  own on-disc menu/navigation logic — a Blu-ray HDMV navigation VM and a DVD
  First-Play resolver — to reach the main feature, instead of guessing by
  title size alone.
- Clip-set composite/decoy detection, so a title assembled from a composite
  or decoy clip set is recognized as such during selection.
- A feature-payload floor, giving main-feature selection a minimum-size
  sanity check.
- Normalized `DiscProfile` output.
- Vendor navigation-label parsing.
- Substantially expanded unit-test coverage across the navigation and
  clip-selection paths.

### Fixed

- Main-feature selection no longer picks an obfuscated decoy playlist over
  the real feature.
- Two rounds of code-audit hardening: navigation VM operand/field-offset
  conformance, navigation now abstains rather than guessing on an
  undecidable system-parameter branch, panic-safety fixes, log-escaping
  fixes, and expanded fuzz/test coverage.
- Direct disc reads (`disc://…`): `DiscStream` now threads a cumulative feed
  base offset into the demuxer (`feed_at`) for both the TS and PS paths and
  forwards it onto each `PesPacket`, so demuxed frames carry byte-exact source
  provenance (`SourcePos`). Without it, `SeamPlan::place` fell back to a PTS
  heuristic that a B-frame PTS reorder dip could misread as a `stepped_back`
  clip transition, dropping all subsequent video frames in multi-clip playlists.

### Changed

- Lowered the minimum supported Rust version (MSRV) from 1.97 to 1.94. This is
  the lowest toolchain on which `cargo build`, `cargo clippy --all-targets
  -D warnings`, and `cargo test --tests` all pass clean; CI toolchain pins were
  moved to match. (The crate's dependencies floor `cargo build` at 1.90, but
  clippy is only warning-clean from 1.94.)

## [1.6.10] — 2026-08-23

### Fixed

- TrueHD/MLP audio: on a source transport-stream discontinuity (lost or gapped
  packets, signalled by a continuity-counter break), the parser now drops forward
  to the next major-sync access unit before resuming, instead of splicing the
  post-gap audio mid-stream. MLP carries predictor + restart state across access
  units, so resuming mid-stream decoded against stale state and produced a
  decoder-choking seam — streaming decoders emitted a burst of "restart header
  sync incorrect" / "Invalid blocksize" errors and could flag the whole track
  corrupt. This mirrors the video path (resync to the next keyframe after a gap)
  and how the independent-frame audio codecs (AC-3, DTS, FLAC, AAC) already
  recover; the fix is guarded so it never arms before a validated major-sync
  baseline exists (no whole-track drop at stream head). The gapped instant itself
  is in the source and cannot be recovered, but the muxed track is now
  decoder-clean.

## [1.6.9] — 2026-08-22

### Changed

- Version aligned to 1.6.9 for the unified release. No functional changes to
  this crate; the release was driven by autorip (automatic per-episode TV
  ripping — each episode named `S{NN}E{MM}`, with TMDB runtime-aligned episode
  numbering across multi-disc seasons — a Manual Rename option, and a unified
  per-disc staging state file — see the autorip 1.6.9 notes).

## [1.6.8] — 2026-08-21

### Changed

- Version aligned to 1.6.8 for the unified release. No functional changes to
  this crate; the release was driven by autorip (webhooks now fire per pipeline
  stage — Rip / Mux / Move — with the Rip hook firing the moment the drive is
  free again, plus a Ripper-tab activity-banner fix so it also shows during
  moves — see the autorip 1.6.8 notes).

## [1.6.7] — 2026-08-21

### Changed

- Version aligned to 1.6.7 for the unified release. No functional changes to
  this crate; the release was driven by autorip (per-webhook event selection,
  a progress bar per moved artifact, and move-queue / webhook-error fixes —
  see the autorip 1.6.7 notes).

## [1.6.6] — 2026-08-20

### Changed

- Version aligned to 1.6.6 for the unified release. No functional changes
  to this crate; the release was driven by autorip (webhooks may now target
  private/LAN addresses — see the autorip 1.6.6 notes).

## [1.6.5] — 2026-08-20

### Fixed

- **On some Blu-ray/UHD titles the picture stopped short of the declared
  end.** When a title is stitched across a clip seam, freemkv drops video
  after the join until it sees a fresh keyframe it can restart decoding
  from. It only recognised one kind of keyframe (an IDR frame), but a
  Blu-ray's final segment can open on a different kind of self-contained
  frame instead — so on those discs the restart point was never
  recognised and every remaining frame to the end of the film was
  dropped, losing up to a group-of-pictures-plus tail of picture. That
  kind of frame is now recognised as a valid restart point, matching how
  the HEVC path already worked, so the tail is kept. HEVC and DVD titles
  were never affected.

- **A split HD-DVD feature could be exported as half the film while still
  claiming the full running time.** A feature stored across parts (for
  example `FEATURE_1.EVO` + `FEATURE_2.EVO`) is composed back into one
  title. If a part resolved with no usable data — a zero-length file, or
  an extent map that yielded nothing — that part was neither used nor
  flagged, so the feature was quietly composed from the surviving parts
  alone, still advertising the whole runtime, and exited success with no
  log. The missing part is now marked unusable and logged under its own
  code (E6019), so half a movie can no longer be presented as a whole one.

- **A Blu-ray title with an unreadable clip could be exported short while
  still reporting the full running time.** A scratched or malformed
  clip-info (`.clpi`) sector, or a clip whose extents could not be
  resolved (a bad sector, a broken allocation chain, an embedded-data
  file), was silently skipped — but the title's duration had already been
  counted from the playlist, so it shipped short of the runtime it claimed
  at exit success, with nothing logged. Previously only one narrow failure
  kind counted; now every unresolvable clip drops the title and warns with
  the read's own error code. A truly absent optional file (such as the 3D
  `.ssif` on a 2D disc) is still treated as benign.

- **Pressing Stop during a disc scan could leave the disc looking like it
  holds fewer titles than it does — or none at all.** Once cancelled,
  every remaining drive command fails, but the Blu-ray and DVD title
  enumerators treated those failures as ordinary skips: the Blu-ray scan
  returned a truncated title list and the DVD scan returned zero titles,
  both at exit success, so a cancelled scan was indistinguishable from a
  disc that simply held that many titles. A cancel is now propagated as an
  error out of every enumerator and at every read site, so it can never be
  cached, displayed, or ripped from as if it were the real disc. HD-DVD
  already behaved this way and is the model the others now follow.

- **A decrypted HD-DVD could be refused with a DVD copy-protection error
  (E7023).** HD-DVD and DVD share the same MPEG program-stream container,
  and the scramble detector mistook an HD-DVD navigation packet for a
  CSS-scrambled sector — so a good, already-decrypted HD-DVD was reported
  as carrying an unrecoverable CSS key and hard-failed, sending anyone
  triaging it hunting for a missing DVD key on a disc that never had one.
  The detector now excludes the structural packet types that CSS never
  scrambles, identified from a field that is readable even on ciphertext,
  so a decrypted HD-DVD scans clean. The DVD CSS crack is unchanged and a
  genuinely scrambled, uncrackable DVD still hard-fails as before, so
  ciphertext can never be muxed as plaintext.

- **A file with an allocated-but-never-written region could splice
  undefined sectors into the rip.** The UDF reader treated an
  allocated-but-unrecorded extent as ordinary content and read whatever
  happened to be on those sectors into the output, and it decoded
  embedded-data files (which store their content inline, not as a sector
  map) as if their bytes were an extent list, pointing the reader at
  unrelated sectors. Such extents are now refused when they actually
  occupy space, and embedded files are handled as their own case; a
  legitimately zero-length file still reads as empty rather than dropping
  its title.

- **Audio and subtitle tracks in many languages were all labelled
  "undefined."** The language mapping recognised only a handful of codes
  and collapsed fifteen others to `und`. Every ISO 639-1 language code is
  now mapped, so those tracks carry their real language. The DVD subtitle
  colour palette, which was written in the wrong order, is also corrected.

- **If the disc read-ahead thread died mid-rip, a whole title could be
  fabricated and reported complete.** A prefetch producer that terminated
  returned an end-of-data signal that the reader legitimately read as a
  short read and zero-filled, so a failed read could be papered over with
  zeros and the pass still reported as successful. It now reports a
  distinct source-terminated error instead. Dead-bus drive faults are
  likewise classified rather than flattened, so the wedged-drive recovery
  path can see them, and the drive now responds to Stop during spin-up and
  spin-down instead of staying deaf for up to ~30 seconds.

- **A holed extraction could climb to a clean 100% on the live progress
  channel.** The progress feed hardcoded its unreadable-byte count to zero
  and counted every zero-filled hole as good data, so a progress-only
  consumer saw a damaged extraction finish spotless — even though the
  authoritative result was already truthful. The real good/unreadable
  split is now threaded through the live channel. Separately, an
  unreadable HD-DVD authored clip order is now logged with its error code
  instead of being silently discarded before falling back to the
  per-clip heuristic.

### Security

- **Bounded several unbounded amplification axes in the HD-DVD and Blu-ray
  scanners.** A crafted disc could drive the playlist nesting depth, the
  title count, and the clips and chapters per title without limit, and
  could force a repeated clip-name fallback probe — each of which alone
  left the worst-case scan unbounded. All now carry positional caps (512
  titles and clips, roughly ten times any retail disc) and the fallback
  probe is memoized. No effect on a well-formed disc.

### Changed

- **How freemkv picks the per-platform drive code was consolidated, with
  no change to reading a disc.** The drive layer chose its
  operating-system-specific module separately inside each entry point; it
  now selects that module once, so a new entry point cannot silently
  forget a platform. Purely internal.

## [1.6.4] — 2026-08-15

### Fixed

- **On a few Blu-ray/UHD titles the sound ran on for half a minute after the
  picture had ended.** A disc stores each part of a film as a clip, and the
  playlist marks exactly where that clip's content begins and ends. Where a
  title is a single clip, freemkv trimmed the picture to those marks but not the
  sound — and some discs leave extra audio in the file past the end mark (a
  quiet fade authored after the last frame of picture). That trailing audio was
  copied through, so the file claimed one running time while carrying up to ~36
  seconds more sound than picture. Measured on `The Bourne Supremacy`: the
  picture ends at 1:48:26 as declared, but every sound track ran to 1:49:02.
  Single-clip titles are now trimmed to their playlist marks the same way
  multi-clip titles already were, so sound and picture end together at the
  declared duration. A title that had no extra material past its marks is
  byte-for-byte unchanged. Multi-clip titles were never affected.

- **A multi-title-set CSS DVD could descramble one title set under another
  set's key.** When a DVD's second title set resisted the keyless title-key
  recovery, the decrypt fell back to the disc-wide key instead of failing —
  writing a corrupt title behind an intact header and reporting success at exit
  0. A failed recovery is now a hard error, the same as every other path in the
  crate already does; ordering makes the recovery more likely to succeed but
  cannot make a failed one safe.

- **A disc whose stream language field was all-zero could abort the whole track
  export.** An all-zero language code (the ordinary "undefined" value on real
  discs) put a NUL byte into a demux output filename and failed file creation
  before a single track opened. Control bytes in that field are now sanitised
  the same way the rest of the name already was.

### Security

- **Bounded the last unbounded attacker-controlled list in the DVD label
  parser.** A crafted IFO could grow the forced-subtitle index list without
  limit; it now carries the same positional cap as the neighbouring command
  lists. No effect on a well-formed disc.

## [1.6.3] — 2026-08-10

### Changed

- **Six crates that were never used have been removed, and nothing about reading
  a disc has changed.** They were declared as dependencies but referenced
  nowhere, so they were compiled into every build for nothing. The rest were
  aligned with the versions the other freemkv crates use.
- **Two complete AES implementations were being built into the product.** Two
  different releases of the cipher crates had been pulled in by different parts
  of the tree and both were compiled. They are now one.

## [1.6.2] — 2026-08-08

### Fixed

- **A stray moment of sound at the end of an HD-DVD title, and a click at every
  chapter break on a DVD.** Both came from the same thing. A disc's sound and
  picture do not arrive in lockstep, and where a title is stitched from
  segments, a few frames of sound can reach the muxer just before or just after
  the picture that marks the join. Those frames were timed against the wrong
  segment. On one HD-DVD title a single trailing sound frame was placed at
  3h33m in a 1h47m film. On a DVD, roughly half a second of sound was squeezed
  into an instant at each of eight chapter breaks — audible as a click, eight
  times in an eight-minute title. Sound is now timed against the segment it
  belongs to. Measured on both discs: the stray frame lands one frame after its
  neighbour, and the DVD's collapsed runs and jumps are gone, with picture
  timing unchanged to the millisecond. Blu-ray was never affected.

## [1.6.1] — 2026-08-07

### Added

- **A disc image can be decrypted without the disc.** `iso://In.iso
  iso://Out.iso` writes a decrypted image from an encrypted one. Ripping from a
  drive is unchanged and still uses the recovery path, because multi-pass retry
  and damage handling exist for media that returns read errors — a file does not.
- **A disc kept as a folder can be read directly.** An extracted `VIDEO_TS` or
  `BDMV` folder works anywhere an image does, as a source or a destination.
  A decrypted backup is judged by its content, not by whether a leftover `AACS`
  directory is present. 3D folders are refused rather than silently mishandled.

### Fixed

- **Blu-ray titles built from several clips ran minutes long, with sound
  drifting ahead of picture.** Such discs store the feature as a chain of clips
  and use the playlist to say which part of each to play. Those marks were never
  read, so skipped stretches became dead time and overlaps put the same moment
  on the timeline twice. One title declared 2h11m and contained 2h13m; the worst
  ran 13 minutes long, with audio adrift from about half an hour in. Every track
  is now placed by the byte offset it was read from, so each clip contributes
  exactly the span the playlist gives it. Measured on four affected titles:
  timelines now land within 12 ms of the declared runtime. Single-clip titles,
  DVDs and HD-DVDs were never affected.
- **A decrypted DVD image could lose most of its title list.** The scrambling
  test read two flag bits and nothing else, which is only meaningful inside an
  MPEG-2 pack. Applied to arbitrary sectors it also matched IFO and filesystem
  structures: on a real disc it destroyed 1912 bytes of the sector carrying
  `TT_SRPT`, so a disc enumerating 38 titles produced an image enumerating 10,
  silently, at exit 0. Descrambling now requires a pack start code.
- **Chapter marks and durations on NTSC DVDs ran about 0.1% short** — some
  3.6 seconds per hour, so a mark near the end of a feature could land seconds
  before the scene it names. DVD times are timecode, and NTSC timecode ticks
  every 30 frames while the video runs at 30000/1001 fps. They are now converted
  through an exact frame count. PAL discs were never affected.

  Reported and fixed by AnimeFN (freemkv#25, libfreemkv#1).

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
