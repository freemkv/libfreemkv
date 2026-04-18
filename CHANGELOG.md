# Changelog

## 0.11.3 (2026-04-18)

### Unified versioning
- All freemkv repos now share the same version number. No functional changes from 0.10.10.

## 0.10.10 (2026-04-18)

### Dual-layer disc fix
- **UDF extent allocation** — use actual UDF allocation descriptors (`file_extents()`) instead of assuming m2ts files are contiguous from `file_start_lba`. Dual-layer UHD discs split large files across many extents (~1 GB each). The old single-extent assumption truncated rips at ~37% on affected discs.
- **Read error propagation** — `fill_extents()` returns `io::Result<bool>` so SCSI read errors propagate to the caller instead of being silently treated as EOF.

## 0.10.9 (2026-04-17)

### Fast disc identification
- **Disc::identify()** — reads UDF filesystem only (name, format, layers, encrypted). ~3s on USB vs 18s for full scan. No AACS handshake or playlist parsing.
- **KEYDB path fix** — added `~/.config/freemkv/keydb.cfg` to search paths. Fixes silent rip hang when KEYDB exists but isn't found by `resolve_keydb()`.

## 0.10.8 (2026-04-17)

### Buffered UDF reads
- **BufferedSectorReader** — prefetches batch sectors on single-sector reads. USB drives have ~500ms per SCSI command; this eliminates scan hangs.
- **Metadata partition pre-read** — loads entire UDF metadata partition into memory after initial parse.
- Scan time reduced from 10+ minutes to ~18 seconds on USB.

## 0.10.7 (2026-04-17)

### DiscStream::new()
- Replaced open_drive(), open_iso(), from_reader() with single new() constructor
- Stream accepts ContentFormat and sets up demuxer internally
- Removed disc:// case from input() — callers use primitives directly

## 0.10.6 (2026-04-16)

### Docker compatibility
- **Drive discovery** — removed sysfs check that blocked detection inside Docker containers. Device nodes are sufficient; INQUIRY command validates the device is an optical drive.

## 0.10.5 (2026-04-16)

### Audio parser buffering
- **AC3** — buffer across PES boundaries with frame size from fscod/frmsizecod table. Eliminates all AC3 decode errors on BD and UHD.
- **DTS** — buffer with core sync detection + frame size from header. DTS-HD extension frames handled correctly.
- **TrueHD** — buffer with unit length field parsing. Incomplete units held for next PES.
- All audio parsers now emit complete frames only. When PES boundaries align (normal case), buffering is a no-op.

## 0.10.4 (2026-04-16)

### CSS decryption — full key hierarchy
- **Bus auth → disc key → title key** — complete CSS key chain. Bus authentication with CSSCryptKey challenge-response, disc key decryption using 31 player keys via READ DVD STRUCTURE, title key extraction via REPORT KEY format 0x04.
- **CSS descramble cipher** — correct LFSR keystream generation with TAB5 for LFSR1 output and TAB4 for LFSR0 output. Per-sector key derivation from title key XOR sector seed.
- **Stevenson plaintext attack** — expanded pattern set (padding, video, audio, nav pack headers), scans up to 50K scrambled sectors for ISO key recovery.
- **Disc::copy() CSS decrypt** — sector-level decryption during disc→ISO copy produces clean ISOs with zero scramble flags.

### MPEG-2 PS demuxer fixes
- **DVD PS path routes through codec parsers** — was bypassing parser.parse(), producing raw PES frames without codec_private extraction or keyframe detection.
- **MPEG-2 sequence header extraction** — calculates exact header size including quantizer matrices (intra/non-intra flags), captures sequence extension from subsequent PES packets.
- **TsDemuxer dynamic PID table** — Vec instead of fixed [i16; 8192] for DVD PIDs that may exceed 8192.

## 0.10.3 (2026-04-16)

### DVD CSS authentication
- **CSS drive authentication** — full SCSI REPORT KEY / SEND KEY handshake with 6-round substitution-permutation cipher (CSSCryptKey). Brute-forces variant from 32 possibilities. Drive serves scrambled sectors after auth completes.
- **CSS auth runs before scan** — chicken-and-egg fix: auth must happen before reading VOB sectors for title key cracking, not after.
- **Remove debug output** — strip temporary eprintln from drive reads and CSS auth.

## 0.10.2 (2026-04-15)

### Fixes
- **Disc::copy() batch overflow** — hardcoded 64-sector batch exceeded BU40N's 60-sector hardware limit, causing every read to fail and trigger 5×30s recovery sleep. Now accepts detected batch size from caller, defaults to 60.
- **IFO PGC parsing** — playback time read from offset 0x04 (correct) instead of 0x02 (nr_programs). Cell BCD time at cell+4 not cell+0. DVD durations now correct.
- **Demuxer flush at EOF** — TS and PS demuxers flushed when source reaches EOF, preventing loss of last PES frame. Applied to DiscStream and M2tsStream.
- **DiscStream demuxer selection** — demuxer set by caller based on content_format (TS for Blu-ray, PS for DVD) instead of unconditionally creating TsDemuxer in from_reader()
- **StdioStream FMKV header** — writes/reads metadata header for roundtrip compatibility through stdio pipes

## 0.10.1 (2026-04-15)

### Architecture: streams are PES, disc.copy() for sector dumps
- **One stream per format, bidirectional PES** — MkvStream, M2tsStream, NetworkStream, StdioStream, NullStream each handle read and write
- **IsoStream merged into DiscStream** — one type for physical drives and ISO files, different SectorReader
- **Disc::copy()** — raw sector dump for disc→ISO, not a stream operation
- **IOStream deleted** — no more byte-level Read/Write on streams
- **ContentReader/OpenDisc deleted** — replaced by DiscStream + PES pipeline
- **CountingStream** — wrapper for progress tracking, no state in streams

### Error codes only — zero English in library
- All `io::Error::new(kind, "english")` replaced with `Error` enum variants
- New error variants: StreamReadOnly, StreamWriteOnly, StreamUrlInvalid, MkvInvalid, NoStreams, etc.
- `From<Error> for io::Error` — clean conversion at system boundaries
- Removed unused error variants: WriteError, ProfileNotFound, NotUnlocked, NotCalibrated, ScsiTimeout, etc.

### Deleted dead code
- `mkvout.rs`, `pesout.rs`, `isowriter.rs` — merged into parent stream types
- `lookahead.rs` usage in MkvStream — replaced by PES direct write
- ContentReader, OpenDisc, open_title() — replaced by PES pipeline
- `open_input()`, `open_output()` — replaced by `input()`, `output()`

## 0.10.0 (2026-04-15)

### PES pipeline
- **Unified Stream trait** — `read()` returns PES frames, `write()` accepts them. One trait for all streams.
- **All streams produce/consume PES frames** — DiscStream, IsoStream, MkvStream, M2tsStream, NetworkStream, StdioStream, NullStream
- **DVD PS demux** — MPEG-2 Program Stream demuxer produces PES frames
- **MKV input stream** — MKV demux produces PES frames
- **Network/stdio PES** — PES serialization over TCP and pipes
- **FileSectorReader** — ISO files implement SectorReader for unified disc/ISO handling

### PES pipeline audit (20 fixes)
- PES serialize: track/length validation, OOM cap (256 MB), stuffing compliance
- TsDemuxer: AF length validation, find_start_code verified
- PTS: marker bit validation, ns→90kHz saturating_mul, round-to-nearest
- AC3/DTS: debug_assert promoted to runtime check
- MKV: block_vint 3-4 byte support, track bounds check
- FMKV: JSON 10 MB cap, PAT section_len underflow guard

### codec_privates refactor
- **codec_privates on DiscTitle** — no separate parameter passing, no `_with_X` method variants
- **Streams-not-files** — MkvStream and M2tsStream take `impl Read`, not `File`/`Seek`
- **M2TS roundtrip fix** — TsMuxer Annex B conversion + codec_private in FMKV header
- **MKV remux fix** — MkvStream returns codec_privates from EBML header
- **Network codec_private fix** — FMKV header carries base64 codec_privates

### Cleanup
- Remove Seek/File dependencies from stream interfaces
- Remove eprintln from library code
- Fix all clippy warnings
- 342 tests pass

## 0.9.0 (2026-04-14)

### Drive recovery + decrypt architecture
- **Drive::read()** — single read method with built-in error recovery (min speed → reset → retry)
- **Decrypt in streams** — streams handle their own decryption via `decrypt_sectors()`. Pipeline just moves bytes.
- **keys() on IOStream** — streams report their own decrypt keys
- **InputOptions** — `--raw` wired through to streams, skips decrypt only
- **decrypt_sectors returns Result** — fail instead of silent corruption
- **Handshake fix** — no longer returns fake success on failure
- **Drive::read_capacity()** — for raw sector dump (disc→ISO)
- **Reset on open** — SgIoTransport resets device on every open
- **Simplified DiscStream** — removed on_error/on_success/Recovery enum

### Platform
- **Rust 1.86 MSRV** pinned in Cargo.toml and CI
- **macOS build fix** — MacScsiTransport marked Send
- **is_multiple_of** — replaced nightly API with stable equivalent

### API changes
- **Drive object** — typed DriveSession API
- **Typed StreamUrl** — URL parsing returns enum, not strings
- **DriveStatus API** — reset(), wait_ready with fallback
- **Granular SCSI queries** — individual methods on DriveSession for capture
- **Profile module public** — for external tools (bdemu)
- **Tray lock/unlock** — exposed on Drive

## 0.8.0 (2026-04-11)

### DVD support
- **Full DVD pipeline** — VIDEO_TS detection, IFO parsing, CSS decryption, MPEG-2 PS demuxing
- **CSS cipher** — Stevenson 1999 table-driven implementation, no keys needed
- **IFO parser** — title sets, PGC chains, cell addresses, audio/subtitle attributes, palette
- **MPEG-2 PS demuxer** — pack headers, PES extraction, private stream 1 sub-streams
- **MPEG-2 video parser** — sequence headers, I-frame detection, codec_private

### 100% codec coverage
- **E-AC-3 (Dolby Digital Plus)** — bsid detection, frame size calculation
- **DTS-HD MA/HR** — extension substream detection and inclusion
- **LPCM** — BD header skip, raw PCM extraction
- **DVD subtitles (VobSub)** — passthrough with IFO palette extraction (YCbCr→RGB)
- **Dolby Vision** — verified RPU NAL type 62 preserved in HEVC passthrough

### MKV improvements
- **Chapters** — MPLS PlayList marks → MKV Chapters element
- **Track flags** — FlagDefault, FlagForced, Language correctly set
- **HEVC codec_private** — profile compatibility and constraint flags from SPS
- **VC-1 codec_private** — resolution parsed from sequence header

### Architecture
- **SectorReader trait** — decouples disc scanning from SCSI
- **Disc::scan_image()** — scan ISO images or any SectorReader
- **resolve_encryption()** — single function handles AACS 1.0/2.0/CSS/none
- **Module refactors** — disc/ (4 files), aacs/ (5 files), drive/ (3 files)
- **Module visibility** — internal modules pub(crate), explicit AACS re-exports

### Streams
- **StdioStream** — stdin/stdout pipe
- **IsoStream** — read/write Blu-ray ISO images with UDF 2.50 filesystem
- **Strict URLs** — all URLs require scheme:// prefix, bare paths rejected
- **total_bytes()** — IOStream reports content size for progress display

### Platform
- **Windows SPTI** — SCSI Pass-Through Interface backend
- **Windows builds** — CI + release workflow for x86_64-pc-windows-msvc
- **macOS drive discovery** — separate from Linux (drive/macos.rs)
- **Stable download URLs** — /latest/download/ with version-free filenames

### Audit fixes (4 rounds, 14→0 critical)
- UDF bounds checking on all disc-sourced offsets
- SCSI: Linux residual underflow, macOS task_status type, Windows buffer zeroing
- AACS: EC mod_inv safe, key reduced mod n, host cert fallback
- DiscStream: persistent read state (was recreating ContentReader per call)
- ISO writer: UDF tag checksums, multi-extent >4GB, reserve AVDP placement
- CSS crack: labeled loop break, polynomial match
- 0 clippy warnings

### Testing
- **327 tests** (was 64 at start)
- CSS/AACS cross-validation against independent AES implementation
- End-to-end MKV mux test with H.264 codec headers

## 0.7.2 (2026-04-11)

### Windows support

- **SPTI backend** (`scsi/windows.rs`) — SCSI_PASS_THROUGH_DIRECT via DeviceIoControl
- **Windows drive discovery** (`drive/windows.rs`) — scans CdRom0-15 + drive letters
- **Platform file separation** — `drive/unix.rs` and `drive/windows.rs`, no inline cfg branches
- **CI** — `cargo check` on windows-latest, actions/checkout@v5

### Test suite

- **177 tests** (was 64) — MPLS, CLPI, H.264, HEVC, AC3, VC1, DTS, TrueHd, PGS, EBML, UDF, disc scanning, streams
- **FEATURES.md** created

### Improvements

- **Stable download URLs** — `/latest/download/freemkv-x86_64-unknown-linux-musl.tar.gz` works forever

## 0.7.1 (2026-04-11)

### SectorReader trait

- **`SectorReader` trait** — decouples disc scanning from SCSI. UDF, MPLS, CLPI, labels, and AACS resolution now work with any sector source.
- **`Disc::scan_image()`** — scan ISO images or any SectorReader. Full title/stream/label/AACS pipeline, no drive required.
- **`resolve_encryption()`** — single function handles AACS 1.0, 2.0, or none. Uses whatever path works (KEYDB VUK, handshake, media key, device key).

### Stream types

- **7 stream types** — Disc, ISO, MKV, M2TS, Network, Stdio, Null
- **`IsoStream`** — read/write Blu-ray ISO images. Uses `Disc::scan_image()` for full UDF parsing (not heuristic scanning).
- **`StdioStream`** — stdin/stdout pipe, format-agnostic
- **Strict URL format** — all URLs require `scheme://path`. Bare paths rejected with clear error messages.
- **Validation** — empty paths, missing ports, read-only/write-only direction errors

### IOStream trait

- `IOStream` trait for all stream types (Read + Write + info + finish)
- `open_input()` / `open_output()` resolve URL strings to stream instances

## 0.7.0 (2026-04-11)

### Stream I/O architecture

- **5 stream types** — Disc, MKV, M2TS, Network, Null
- **`IOStream` trait** — common interface for all streams
- **URL resolver** — `open_input()` / `open_output()` with scheme://path format
- **FMKV metadata header** — JSON metadata embedded in M2TS and network streams
- **Bidirectional MKV** — MkvStream reads and writes Matroska containers
- **Network streaming** — TCP with metadata header, TCP_NODELAY
- **BD-TS demuxer** — PAT/PMT scanning, PTS duration detection
- **EBML reader** — parse existing MKV files for read-side MkvStream

## 0.6.0 (2026-04-10)

### API improvements

- **`open()` works on all drives** — no profile match required. Unknown drives can scan, read BD/DVD at OEM speed. `init()` is optional and adds features (riplock removal, UHD reads, speed control).
- **`has_profile()`** — check if unlock parameters are available for this drive
- **`find_drives()`** — returns all optical drives, not just profile-matched ones
- **`raw_gc_010c`** on `DriveId` — raw GET_CONFIG 010C response bytes for profile sharing

### AACS 2.0

- **SCSI handshake wired end-to-end** — ECDH key agreement, real Volume ID from drive, read data key for bus decryption
- **Bus decryption active** — UHD discs with bus encryption now decrypted transparently
- **VUK derivation from Media Key + VID** — works for discs not in KEYDB (processing key + device key paths)

### MKV muxer

- **15 new files** — EBML writer, TS demuxer, stream assembly pipeline
- **Codec parsers** — H.264, HEVC, AC-3, DTS, TrueHD, PGS, VC-1
- **`MkvStream`** — builder pattern, wraps any `impl Write`, configurable lookahead buffer

### Cleanup

- Removed orphaned `jar.rs` (342 lines) — replaced by `labels/` module
- Error refactor: 40+ sites converted from English strings to typed error codes

## 0.5.0 (2026-04-09)

### Read pipeline — 5x speed improvement

- **Kernel transfer limit detection**: auto-detect `max_hw_sectors_kb` via sysfs, resolve sg→block device. Previously hardcoded to 510 sectors (1MB) which exceeded the 120KB kernel limit, causing all reads to error and fall back to 6KB reads at 4.8 MB/s. Now auto-tunes to 48 sectors (96KB) or whatever the device supports.
- **Result: 12.5 MB/s sustained, 23 MB/s peak** (was 4.8 MB/s)

### LibreDrive — full init pipeline

- **All 10 ARM handlers translated**: unlock, firmware upload (A: WRITE_BUFFER, B: MODE SELECT), calibrate (256 zones), register reads, status, probe, set_read_speed, keepalive, timing
- **Cold boot firmware upload**: WRITE_BUFFER 1888B (A variant) or MODE SELECT 2496B (B variant) proven on hardware
- **Speed calibration**: 256+ disc surface probes, 64-entry speed table, triple SET_CD_SPEED
- **Platform trait locked down**: `pub(crate)`, 3 methods only (init, set_read_speed, is_ready)
- **Init guard**: prevents double-init, signature mismatch aborts early

### MPLS parser fixes

- **PGS in audio slots**: subtitle language read at correct offset (was truncated: "ng " → "eng")
- **Secondary PG entries**: n_pip_pg loop added for correct STN position tracking
- **Secondary stream types**: stream_type 5 (sec audio), 6 (sec video), 7 (DV EL) attribute parsing
- **Empty stream filter**: coding_type 0x00 entries (padding) no longer appear as "Unknown(0)"

### Profiles

- **206 profiles with full per-drive data**: ld_microcode (base64), all CDBs, speed tables, signatures
- **Automated pipeline**: `sdf_unpack --profiles` → profiles.json (no manual merging)

## 0.4.0 (2026-04-07)

### Labels — complete rewrite

- **Detect-then-parse architecture**: each BD-J authoring format has its own parser module with `detect()` and `parse()` functions. Drop in a new parser with one line in the registry.
- **5 format parsers**: Paramount (`playlists.xml`), Criterion (`streamproperties.xml`), Pixelogic (`bluray_project.bin`), Warner CTRM (`menu_base.prop` / `language_streams.txt`), shared label vocabulary (`vocab.rs`)
- **Raw disc data principle**: label data passes through as-is from disc. Only BD-standard codec identifiers (MLP, AC3, DTS) are mapped to display names. Unknown authoring tool codes (csp, eda, cf) pass through raw.
- **`variant` field**: replaces `region` — language dialect codes from authoring tools, not BD spec regions
- Removed: old `jar` module (superseded by labels)
- Removed: dead label apply functions from disc.rs

### Drive

- **`DriveSession::eject()`**: sends PREVENT ALLOW MEDIUM REMOVAL then START STOP UNIT. Works reliably after raw mode unlock.
- **`DiscRegion` enum**: Free, BluRay(A/B/C), Dvd(1-8). UHD always region-free.

### Capture

- **Fixed sector range collection**: captures ALL files on disc (only skips STREAM/ video files and >50MB). Previously skipped BACKUP/, DUPLICATE/, and files >10MB which missed JAR content.

## 0.3.1

- Labels module: 4 disc file parsers for stream labels
- Simplified labels API

## 0.3.0

- Initial public release
- SCSI transport (Linux SG_IO, macOS IOKit)
- UDF 2.50 filesystem reader
- MPLS/CLPI parsers with full STN support
- Drive identification + profile matching
- 206 bundled drive profiles
- AACS 1.0 decryption (VUK + unit keys)
