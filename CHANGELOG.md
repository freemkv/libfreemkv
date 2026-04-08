# Changelog

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
