[![Crates.io](https://img.shields.io/crates/v/libfreemkv)](https://crates.io/crates/libfreemkv)
[![docs.rs](https://img.shields.io/docsrs/libfreemkv)](https://docs.rs/libfreemkv)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

# libfreemkv

Rust library for 4K UHD / Blu-ray / DVD optical drives. Drive access, disc scanning, stream labels, AACS decryption, CSS decryption, KEYDB updates, and content reading in one crate. Bundled drive profiles — no external files needed.

**12+ MB/s** sustained read speeds on BD. Full init: unlock, firmware upload, speed calibration — all from pure Rust.

Multi-lingual by design — the library outputs structured data and numeric error codes, never English text. Build any UI or localization on top.

**[API Documentation](https://docs.rs/libfreemkv)** · **[Technical Docs](docs/)**

Part of the [freemkv](https://github.com/freemkv) project.

## Install

```toml
[dependencies]
libfreemkv = "0.11"
```

## Quick Start

```rust
use libfreemkv::{Drive, Disc, ScanOptions};
use std::path::Path;

// Open drive — profiles are bundled, auto-identified
let mut drive = Drive::open(Path::new("/dev/sg4"))?;
drive.wait_ready()?;              // wait for disc
drive.init()?;                     // unlock + firmware upload
drive.probe_disc()?;               // probe disc surface for optimal speeds

// Scan disc — UDF, playlists, streams, AACS (all automatic)
let disc = Disc::scan(&mut drive, &ScanOptions::default())?;

for title in &disc.titles {
    println!("{} — {} streams", title.duration_display(), title.streams.len());
}

// Stream pipeline — read PES frames from any source, write to any output
let opts = libfreemkv::InputOptions::default();
let mut input = libfreemkv::input("iso://Disc.iso", &opts)?;
let title = input.info().clone();
let mut output = libfreemkv::output("mkv://Movie.mkv", &title)?;
while let Ok(Some(frame)) = input.read() {
    output.write(&frame)?;
}
output.finish()?;
```

### Multi-pass recovery rip

For damaged discs, the library offers a two-stage rip model: fast sweep with zero-fill and a ddrescue-format mapfile, then targeted retry of bad ranges. See [`docs/rip-recovery.md`](docs/rip-recovery.md) for the full architecture.

```rust
use libfreemkv::disc::{CopyOptions, PatchOptions};

// Pass 1: disc → ISO. Fast 64 KB reads, skip-forward on failure,
// zero-fill bad blocks, write a sidecar .mapfile.
let mut result = disc.copy(
    &mut drive,
    Path::new("disc.iso"),
    &CopyOptions { skip_on_error: true, skip_forward: true, ..Default::default() },
)?;

// Pass 2..N: retry bad ranges with full drive recovery.
// Idempotent — call as many times as you want.
while result.bytes_unreadable + result.bytes_pending > 0 {
    let pr = disc.patch(&mut drive, Path::new("disc.iso"), &PatchOptions::default())?;
    if pr.bytes_recovered_this_pass == 0 { break; }
}

// Then mux from the ISO via the normal stream pipeline (no drive involvement).
```

## What It Does

- **Drive access** — open, identify, unlock, firmware upload, speed calibration, eject
- **12+ MB/s reads** — auto-detects kernel transfer limits, sustained full speed
- **Disc scanning** — UDF 2.50 filesystem, MPLS playlists, CLPI clip info
- **Stream labels** — 5 BD-J format parsers (Paramount, Criterion, Pixelogic, CTRM, Deluxe)
- **AACS decryption** — transparent key resolution and content decrypt (1.0 + 2.0 bus decryption)
- **KEYDB updates** — download, verify, save from any HTTP URL (zero deps, raw TCP)
- **Content reading** — adaptive batch reads with automatic decryption and error recovery
- **Stream I/O** — unified stream pipeline for reading and writing any format

### Streams

| Stream | Input | Output | Transport |
|--------|-------|--------|-----------|
| DiscStream | Yes | -- | Optical drive via SCSI |
| IsoStream | Yes | Yes | Blu-ray ISO image file |
| MkvStream | Yes | Yes | Matroska container |
| M2tsStream | Yes | Yes | BD transport stream with FMKV metadata header |
| NetworkStream | Yes (listen) | Yes (connect) | TCP with FMKV metadata header |
| StdioStream | Yes (stdin) | Yes (stdout) | Raw byte pipe |
| NullStream | -- | Yes | Discard sink (byte counter for benchmarks) |

Streams implement `IOStream` (byte-level) and `pes::Stream` (frame-level). `input()` / `output()` resolve URL strings to PES stream instances. `open_input()` / `open_output()` resolve to byte-level IOStream instances. All URLs use the `scheme://path` format — bare paths are rejected.

AACS decryption requires a KEYDB.cfg file. If available at `~/.config/aacs/KEYDB.cfg` or passed via `ScanOptions`, the library handles everything — handshake, key derivation, and per-sector decryption — without the application needing to know anything about encryption.

## Architecture

```text
Drive                  — open, identify, init, unlock, read (with recovery)
  ├── ScsiTransport    — SG_IO (Linux), IOKit (macOS), SPTI (Windows)
  ├── DriveProfile     — per-drive unlock parameters (bundled)
  └── PlatformDriver   — MediaTek (supported), Renesas (planned)

Disc                   — scan titles, streams, AACS/CSS state
  ├── UDF reader       — Blu-ray UDF 2.50 with metadata partitions
  ├── MPLS parser      — playlists → titles + clips + streams
  ├── CLPI parser      — clip info → EP map → sector extents
  ├── IFO parser       — DVD title sets, PGC chains, cell addresses
  ├── Labels           — 5 BD-J format parsers (detect + parse)
  ├── AACS             — key resolution + content decryption
  ├── CSS              — DVD CSS cipher (table-driven, no keys needed)
  └── KEYDB            — download + verify + save

Streams                — unified PES pipeline
  ├── pes::Stream      — read()/write() PES frames
  ├── DiscStream       — sectors → decrypt → TS demux → PES
  ├── IsoStream        — ISO file → decrypt → TS demux → PES
  ├── MkvStream        — MKV mux/demux
  ├── M2tsStream       — BD transport stream
  ├── NetworkStream    — TCP with FMKV metadata header
  ├── StdioStream      — stdin/stdout pipe
  └── NullStream       — discard sink
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
| Windows | Supported | SPTI |

## Contributing

Run `freemkv info disc:// --share` with the [freemkv CLI](https://github.com/freemkv/freemkv) to contribute your drive's profile.

## License

AGPL-3.0-only
