# Changelog

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
