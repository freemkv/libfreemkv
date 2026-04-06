[![Crates.io](https://img.shields.io/crates/v/libfreemkv)](https://crates.io/crates/libfreemkv)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)
[![Drives: 206](https://img.shields.io/badge/drives-206-green)]()

# libfreemkv

Open source raw disc access library for UHD Blu-ray optical drives.

Enables direct sector reading on compatible drives for UHD Blu-ray archival,
backup, and media extraction. Ships with community-contributed drive profiles —
no proprietary data files needed at runtime.

## Features

- **Drive identification** — SCSI INQUIRY + GET CONFIGURATION for automatic profile matching
- **Raw read mode** — activate enhanced read mode on supported drives
- **Speed calibration** — optimal read speed per disc region
- **Raw sector reading** — direct READ(10) access to disc sectors
- **Drive profiles** — per-drive SCSI command data, shipped as JSON files
- **Community-driven** — submit new drive profiles via `freemkv-info`

## Supported Drives

Currently supports 280+ LG, ASUS, and HP optical drive firmware versions
across the MediaTek MT1959 chipset family. Pioneer Renesas support is in progress.

See [profiles/](profiles/) for the full list.

## Installation

```bash
cargo install libfreemkv
```

Or add to your `Cargo.toml`:

```toml
[dependencies]
libfreemkv = "0.1"
```

## Quick Start

### As a library

```rust
use libfreemkv::DriveSession;
use std::path::Path;

let mut session = DriveSession::open(
    Path::new("/dev/sr0"),
    Path::new("profiles/"),
)?;

session.enable()?;        // activate raw read mode
session.calibrate()?;     // optimize read speed

let mut buf = vec![0u8; 2048];
session.read_sectors(0, 1, &mut buf)?;
```

### freemkv-info

Identify your drive and check compatibility:

```bash
$ freemkv-info /dev/sr0
Drive: HL-DT-ST BD-RE BU40N 1.03
Chipset: MT1959
Raw Read: Supported
Profile: Found (mt1959_a)

$ freemkv-info /dev/sr0 --raw
# Dumps full INQUIRY and GET CONFIGURATION responses as hex
# Useful for contributing profiles for unsupported drives
```

### freemkv-test

Verify raw read mode works:

```bash
$ freemkv-test /dev/sr0
Enabling raw read mode... OK
Calibrating speed... OK (42 speed zones)
Reading sector 0... OK (2048 bytes)
Reading sector 1000... OK (2048 bytes)
All checks passed.
```

## Contributing Drive Profiles

If your drive isn't supported, you can help:

1. Run `freemkv-info /dev/sr0 --raw > my_drive.txt`
2. Open an issue or PR with the output
3. We'll generate a profile from your drive data

This is especially needed for Pioneer drives.

## Architecture

```
DriveSession
├── ScsiTransport     — SG_IO (Linux) / IOKit (macOS)
├── DriveProfile      — per-drive JSON data
└── Platform          — per-chipset unlock + read logic
    ├── Mt1959        — LG/ASUS MediaTek drives
    └── Pioneer       — Pioneer Renesas drives (WIP)
```

The library implements 10 drive commands per platform:

| Command | Purpose |
|---------|---------|
| enable | Activate raw read mode |
| read_config | Read drive configuration |
| read_register | Read hardware registers |
| calibrate | Build speed optimization table |
| keepalive | Session keepalive |
| status | Read mode status and features |
| probe | Generic drive query |
| read_sectors | Read raw disc sectors |
| read_disc_structure | Read disc metadata |
| timing | Timing calibration |

## License

AGPL-3.0-only
