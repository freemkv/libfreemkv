# libfreemkv Crate Overview

Overflow detail for the `src/lib.rs` crate-level doc comment, which is capped
by `ci/comment-guard.py`. Pointed to by the `//!` header there.

## Unlocking

Unlocking -- removing bus encryption (firmware unlock, AACS cert handshake,
CSS bus-auth) -- lives entirely in the `freemkv-unlock` crate; libfreemkv
consumes it privately and exposes none of it, so clients are oblivious to
unlockers (just as they are to the SCSI layer).

## Why a live `disc://` can't go through `input()`

A live `disc://` cannot be opened via [`input`] -- it returns
[`Error::DiscUrlNotDirect`] by design (use `Drive` + `Disc::scan` +
`DiscStream::new` directly for a live drive). Any file-backed source
(`iso://`, `m2ts://`) opens through [`input`].

## Architecture

```text
Drive           -- open, identify, unlock, read sectors
  ├── ScsiTransport    -- SG_IO (Linux), IOKit (macOS)
  ├── DriveId          -- INQUIRY + GET_CONFIG identification
  └── unlock_bridge    -- private seam to the `freemkv-unlock` crate
                          (firmware / AACS cert / CSS bus-auth unlockers)

Disc                   -- scan titles, streams, AACS state
  ├── UDF reader       -- Blu-ray UDF 2.50 with metadata partitions
  ├── MPLS parser      -- playlists → titles + clips + STN streams
  ├── CLPI parser      -- clip info → EP map → sector extents
  ├── JAR parser       -- BD-J audio track labels
  └── AACS             -- encryption: key resolution + content decrypt
      ├── aacs         -- KEYDB, VUK, MKB, unit decrypt
      └── host_certs   -- collect host certs (cert handshake lives in freemkv-unlock)
```

See also `docs/architecture.md` for the fuller design-level writeup.

## AACS Encryption

Disc scanning automatically detects and handles AACS encryption.
If a KEYDB.cfg is available (via `ScanOptions` or standard paths),
the library resolves keys and decrypts content transparently.

Supports AACS 1.0 (Blu-ray) and AACS 2.0 (UHD, with fallback).

## Error Codes

All errors are structured with numeric codes. No user-facing English
text -- applications format their own messages. See `docs/error-codes.md`
for the full range table.
