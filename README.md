[![Crates.io](https://img.shields.io/crates/v/libfreemkv)](https://crates.io/crates/libfreemkv)
[![docs.rs](https://img.shields.io/docsrs/libfreemkv)](https://docs.rs/libfreemkv)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

# libfreemkv

Rust library for 4K UHD / Blu-ray optical drives. Drive access, disc scanning, stream labels, AACS decryption, KEYDB updates, and content reading in one crate. Bundled drive profiles — no external files needed.

**12+ MB/s** sustained read speeds on BD. Full init: unlock, firmware upload, speed calibration — all from pure Rust.

Multi-lingual by design — the library outputs structured data and numeric error codes, never English text. Build any UI or localization on top.

**[API Documentation](https://docs.rs/libfreemkv)** · **[Technical Docs](docs/)**

Part of the [freemkv](https://github.com/freemkv) project.

## Install

```toml
[dependencies]
libfreemkv = "0.6"
```

## Quick Start

```rust
use libfreemkv::{DriveSession, Disc, ScanOptions};
use std::path::Path;

// Open drive — profiles are bundled, auto-identified
let mut session = DriveSession::open(Path::new("/dev/sr0"))?;
session.wait_ready()?;            // wait for disc
session.init()?;                   // unlock + firmware upload
session.probe_disc()?;             // probe disc surface for optimal speeds

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

- **Drive access** — open, identify, unlock, firmware upload, speed calibration, eject
- **12+ MB/s reads** — auto-detects kernel transfer limits, sustained full speed
- **Disc scanning** — UDF 2.50 filesystem, MPLS playlists, CLPI clip info
- **Stream labels** — 5 BD-J format parsers (Paramount, Criterion, Pixelogic, CTRM, Deluxe)
- **AACS decryption** — transparent key resolution and content decrypt (1.0 + 2.0 bus decryption)
- **KEYDB updates** — download, verify, save from any HTTP URL (zero deps, raw TCP)
- **Content reading** — adaptive batch reads with automatic decryption and error recovery

AACS decryption requires a KEYDB.cfg file. If available at `~/.config/aacs/KEYDB.cfg` or passed via `ScanOptions`, the library handles everything — handshake, key derivation, and per-sector decryption — without the application needing to know anything about encryption.

## Architecture

```text
DriveSession           — open any drive, identify, init (optional), read sectors
  ├── ScsiTransport    — SG_IO (Linux), IOKit (macOS)
  ├── DriveProfile     — per-drive unlock parameters (bundled)
  └── PlatformDriver   — MediaTek (supported), Renesas (planned)

Disc                   — scan titles, streams, AACS state
  ├── UDF reader       — Blu-ray UDF 2.50 with metadata partitions
  ├── MPLS parser      — playlists → titles + clips + streams
  ├── CLPI parser      — clip info → EP map → sector extents
  ├── Labels           — 5 BD-J format parsers (detect + parse)
  ├── AACS             — key resolution + content decryption
  └── KEYDB            — download + verify + save
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
| E8xxx | KEYDB update errors |

## Platform Support

| Platform | Status | Backend |
|----------|--------|---------|
| Linux | Supported | SG_IO ioctl |
| macOS | Supported | IOKit SCSITask |
| Windows | Planned | SPTI |

## Contributing

Run `freemkv drive-info --share` with the [freemkv CLI](https://github.com/freemkv/freemkv) to contribute your drive's profile.

## License

AGPL-3.0-only
