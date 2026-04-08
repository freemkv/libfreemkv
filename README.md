[![Crates.io](https://img.shields.io/crates/v/libfreemkv)](https://crates.io/crates/libfreemkv)
[![docs.rs](https://img.shields.io/docsrs/libfreemkv)](https://docs.rs/libfreemkv)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

# libfreemkv

Rust library for 4K UHD / Blu-ray optical drives. Drive access, disc scanning, AACS decryption, and content reading in one crate. Bundled drive profiles — no external files needed.

**[API Documentation](https://docs.rs/libfreemkv)** · **[Technical Docs](docs/)**

Part of the [freemkv](https://github.com/freemkv) project.

## Install

```toml
[dependencies]
libfreemkv = "0.4"
```

## Quick Start

```rust
use libfreemkv::{DriveSession, Disc, ScanOptions};
use std::path::Path;

// Open drive — profiles are bundled, auto-identified
let mut session = DriveSession::open(Path::new("/dev/sr0"))?;

// Scan disc — UDF, playlists, streams, AACS (all automatic)
let disc = Disc::scan(&mut session, &ScanOptions::default())?;

for title in &disc.titles {
    println!("{} — {} streams", title.duration_display(), title.streams.len());
}

// Read content (decrypted transparently if AACS keys available)
let mut reader = disc.open_title(&mut session, 0)?;
while let Some(unit) = reader.read_unit()? {
    // 6144 bytes of content per aligned unit
}
```

## What It Does

- **Drive access** — open, identify, unlock for raw reads
- **Disc scanning** — UDF 2.50 filesystem, MPLS playlists, CLPI clip info, BD-J labels
- **AACS decryption** — transparent key resolution and content decrypt (1.0 + 2.0)
- **Content reading** — sector reads with automatic decryption

AACS decryption requires a KEYDB.cfg file. If available at `~/.config/aacs/KEYDB.cfg` or passed via `ScanOptions`, the library handles everything — handshake, key derivation, and per-sector decryption — without the application needing to know anything about encryption.

## Architecture

```text
DriveSession           — open, identify, unlock, read sectors
  ├── ScsiTransport    — SG_IO (Linux), IOKit (macOS)
  ├── DriveProfile     — per-drive unlock parameters (bundled)
  └── Platform         — MediaTek (supported), Renesas (planned)

Disc                   — scan titles, streams, AACS state
  ├── UDF reader       — Blu-ray UDF 2.50 with metadata partitions
  ├── MPLS parser      — playlists → titles + clips + streams
  ├── CLPI parser      — clip info → EP map → sector extents
  ├── JAR parser       — BD-J audio track labels
  └── AACS             — key resolution + content decryption
```

See [docs/](docs/) for detailed technical documentation on each module.

## Error Codes

All errors are structured with numeric codes. No user-facing English text — applications format their own messages.

| Range | Category |
|-------|----------|
| E1xxx | Device errors (not found, permission) |
| E2xxx | Profile errors (unsupported drive) |
| E3xxx | Unlock errors (failed, signature) |
| E4xxx | SCSI errors (command failed, timeout) |
| E5xxx | I/O errors |
| E6xxx | Disc format errors |
| E7xxx | AACS errors |

## Platform Support

| Platform | Status | Backend |
|----------|--------|---------|
| Linux | Supported | SG_IO ioctl |
| macOS | Supported | IOKit SCSITask |
| Windows | Planned | SPTI |

## Contributing

Run `freemkv info --share` with the [freemkv CLI](https://github.com/freemkv/freemkv) to contribute your drive's profile.

## License

AGPL-3.0-only
