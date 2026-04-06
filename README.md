[![Crates.io](https://img.shields.io/crates/v/libfreemkv)](https://crates.io/crates/libfreemkv)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)
[![Drives: 206](https://img.shields.io/badge/drives-206-brightgreen)]()

# libfreemkv

Rust library for raw sector access on optical drives. Identifies drives using standard SCSI commands, matches them against 206 bundled profiles, and unlocks raw read mode. No external files, no configuration.

Part of the [freemkv](https://github.com/freemkv) project.

## Install

```toml
[dependencies]
libfreemkv = "0.1"
```

## Usage

```rust
use libfreemkv::DriveSession;
use std::path::Path;

let mut session = DriveSession::open(Path::new("/dev/sr0"))?;

session.unlock()?;       // activate raw read mode
session.calibrate()?;    // optimize read speed

let mut buf = vec![0u8; 2048];
let n = session.read_sectors(0, 1, &mut buf)?;
```

## How It Works

1. **INQUIRY** (SPC-4 §6.4) — reads vendor_id, product_id, product_revision, vendor_specific
2. **GET CONFIGURATION 010C** (MMC-6 §5.3.10) — reads firmware_date
3. **Profile match** — five fields uniquely identify the drive against 206 bundled profiles
4. **READ BUFFER** — single command with per-drive mode/buffer_id activates raw read mode
5. **Signature verify** — drive responds with 4-byte signature + "MMkv" confirmation

No fingerprints. No encrypted lookups. All identification uses standard SCSI fields.

## API

```rust
// Open and auto-identify
let mut session = DriveSession::open(device_path)?;

// Drive identity
session.drive_id.vendor_id       // "HL-DT-ST"
session.drive_id.product_id      // "BD-RE BU40N"
session.drive_id.product_revision // "1.03"
session.drive_id.vendor_specific  // "NM00000"
session.drive_id.firmware_date    // "211810241934"

// Profile data
session.profile.chipset           // Chipset::MediaTek
session.profile.unlock_mode       // 0x01
session.profile.unlock_buf_id     // 0x44
session.profile.signature         // [0x99, 0x9e, 0xc3, 0x75]

// Operations
session.unlock()?;                // activate raw mode
session.calibrate()?;             // speed optimization
session.read_sectors(lba, count, &mut buf)?;
session.status()?;                // feature flags
session.read_config()?;           // drive configuration
session.read_register(index)?;    // hardware registers
```

## Chipset Support

| Chipset | Status | Drives | Brands |
|---------|--------|--------|--------|
| MediaTek MT1959 | Supported | 206 | LG, ASUS, HP |
| Renesas | Planned | -- | Pioneer |

Each profile stores per-drive `unlock_mode` and `unlock_buf_id` — the exact CDB bytes for that drive's unlock command. A single `Mt1959` implementation handles all MediaTek variants.

## Drive Profile

Each bundled profile (compiled into the binary):

```json
{
  "vendor_id": "HL-DT-ST",
  "product_id": "BD-RE BU40N     ",
  "product_revision": "1.03",
  "vendor_specific": "NM00000",
  "firmware_date": "211810241934",
  "chipset": "mediatek",
  "unlock_mode": 1,
  "unlock_buf_id": 68,
  "signature": "999ec375",
  "register_offsets": ["10e291", "11ab1c"]
}
```

## Error Codes

Structured errors for programmatic handling — no user-facing English text in the library.

| Code | Error | Meaning |
|------|-------|---------|
| E1000 | DeviceNotFound | Device path doesn't exist |
| E1001 | DevicePermission | No access (try sudo or cdrom group) |
| E2000 | UnsupportedDrive | No matching profile |
| E2001 | ProfileNotFound | Profile lookup failed |
| E3000 | UnlockFailed | Unlock command rejected |
| E3001 | SignatureMismatch | Response signature wrong |
| E3002 | NotUnlocked | Operation requires unlock first |
| E4000 | ScsiError | SCSI command failed |
| E5000 | IoError | System I/O error |

## Architecture

```
DriveSession
├── ScsiTransport     SG_IO (Linux) / IOKit (macOS, planned)
├── DriveProfile      per-drive parameters (bundled JSON, compiled in)
├── DriveId           INQUIRY + GET_CONFIG 010C fields
└── Platform
    ├── Mt1959        MediaTek MT1959 (206 drives)
    └── (Renesas)     Pioneer (planned)
```

## Platform

Linux only today (SG_IO ioctl). The `ScsiTransport` trait abstracts the platform — macOS IOKit and Windows SPTI backends are planned.

## Contributing

Run `freemkv info --share` with the [freemkv CLI](https://github.com/freemkv/freemkv) to submit your drive's profile.

## License

AGPL-3.0-only
