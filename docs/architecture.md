# libfreemkv Architecture

Open source optical drive access library for 4K UHD Blu-ray, Blu-ray, and DVD.
Rust library with no external dependencies at runtime -- profiles are bundled,
AACS keys are derived internally, and all SCSI communication is handled in-process.

**Repository:** <https://github.com/freemkv/libfreemkv>
**License:** AGPL-3.0-only

---

## Design Principles

1. **CLI is dumb.** All drive communication, disc parsing, AACS decryption, and
   format handling live in the library. CLI binaries are thin wrappers that call
   `Drive::open()` and `Disc::scan()`.

2. **No external files.** Bundled drive profiles are compiled into the binary via
   `include_str!`. No configuration directory, no runtime file lookups for drive
   support.

3. **Transparent AACS.** The `ContentReader` decrypts on the fly when keys are
   available. Callers read cleartext sectors without knowing whether the disc
   was encrypted.

4. **Structured errors, no English.** Every error has a numeric code (E1000-E8000).
   The library never formats user-facing messages -- applications do that.

5. **Library-agnostic.** No concept of "supported" vs "unsupported" drives at a
   policy level. If a profile exists, the library uses it.

6. **Streams are dumb pipes.** Streams read/write PES frames. They don't know
   about encryption, transport format, or source type. Decrypt is a stream-internal
   concern; the pipeline just moves frames.

---

## Module Map

```
libfreemkv (lib.rs)
│
├── Drive Access
│   ├── drive         Drive — open, identify, init, unlock, single-shot read
│   ├── scsi          ScsiTransport trait + platform backends (sg async, IOKit, SPTI)
│   ├── platform/     Platform trait — per-chipset command handlers
│   │   └── mt1959    MediaTek MT1959 driver (LG, ASUS, HP)
│   ├── profile       DriveProfile loading, matching, bundled JSON
│   ├── identity      DriveId from INQUIRY + GET_CONFIG 010C
│   ├── speed         DriveSpeed enum, SET CD SPEED CDB builder
│   └── event         Event system for drive status callbacks
│
├── Disc Scanning
│   ├── disc          Disc::scan() — titles, streams, extents, AACS setup
│   ├── udf           UDF 2.50 filesystem reader (metadata partitions)
│   ├── mpls          MPLS playlist parser — clips, streams, STN table
│   ├── clpi          CLPI clip info parser — EP map, sector extents
│   ├── ifo           DVD IFO parser — title sets, PGC chains, cell addresses
│   └── labels/       BD-J label extraction (5 formats: Paramount, Criterion, Pixelogic, CTRM, Deluxe)
│
├── Encryption
│   ├── aacs/         AACS handshake, KEYDB, VUK lookup, MKB, unit decryption
│   ├── css           DVD CSS cipher — table-driven, no external keys needed
│   └── decrypt       decrypt_sectors() — unified AACS/CSS/None dispatcher
│
├── Streaming
│   ├── mux/          Stream implementations (Disc, ISO, MKV, M2TS, Network, Stdio, Null)
│   ├── pes           PES frame types; FrameSource / FrameSink direction-typed traits
│   └── sector/       SectorSource / SectorSink traits, FileSector{Source,Sink}, DecryptingSectorSource
│
├── I/O Primitives
│   ├── halt          Halt cancellation token (one Arc<AtomicBool>, cloneable)
│   └── io/           Pipeline<I, R> + Sink trait + WritebackFile (bounded-cache writer)
│
├── Support
│   ├── keydb         KEYDB.cfg download, parse, verify, save
│   ├── error         Error enum with numeric codes E1000-E8000
│   └── profile       Bundled drive profiles
│
└── lib.rs            Public API re-exports
```

---

## Drive Access Flow

```
Drive::open(Path::new("/dev/sg4"))
  │
  ├─ scsi::open()           Open /dev/sg4 (async write/poll/read)
  ├─ DriveId::from_drive()  INQUIRY + GET_CONFIG 010C
  ├─ profile::find_by_drive_id()  Match against bundled profiles
  ├─ Platform::new()        Instantiate chipset driver (Mt1959)
  └─ Drive ready for init/unlock/read
```

After open:
- `init()` -- unlock + firmware upload + speed calibration
- `probe_disc()` -- probe disc surface for optimal speeds
- `read(lba, count, buf, recovery)` -- single-shot read; `recovery` only selects the per-CDB timeout (1.5 s vs. 30 s)
- `wait_ready()` -- wait for disc insertion
- `eject()` -- eject tray

Recovery is layered above `Drive::read`, not inside it. Layer 1
(`Disc::patch`) handles bad-range retry by replaying the ddrescue mapfile.
Layer 3 (`DiscStream::fill_extents` adaptive batch sizer) handles in-loop
request-size adaptation. Inline recovery (gentle retry → SCSI reset → retry)
was removed in 0.13.6 — see [`rip-recovery.md`](rip-recovery.md) and
the stop-wedge postmortem (2026-04-25).

---

## Disc Scanning Flow

```
Disc::scan(&mut drive, &ScanOptions)
  │
  ├─ READ CAPACITY          Get disc size in sectors
  ├─ udf::read_filesystem() Parse UDF 2.50 (AVDP → VDS → metadata → FSD → root)
  ├─ For each BDMV/PLAYLIST/*.mpls:
  │   ├─ mpls::parse()      Extract play items, STN streams
  │   └─ For each clip:
  │       └─ clpi::parse()  EP map → sector extents for the clip's time range
  ├─ labels::detect()       Parse BD-J JARs for stream labels
  ├─ Detect AACS            Check for /AACS directory on disc
  └─ Disc::setup_aacs()     Handshake + KEYDB → VUK → unit keys (if encrypted)
```

For DVD:
```
Disc::scan_dvd(&mut drive, &ScanOptions)
  │
  ├─ ifo::parse()           Parse VIDEO_TS.IFO — title sets, PGC chains
  ├─ CSS detection          Check disc structure flag
  └─ CSS key cracking       Table-driven, no KEYDB needed
```

The result is a `Disc` with:
- `titles: Vec<DiscTitle>` -- sorted by duration, each with streams, sector extents, codec_privates
- `decrypt_keys()` -- DecryptKeys for content decryption
- `encrypted: bool` -- whether the disc uses AACS/CSS

---

## AACS Decryption

Four key resolution paths, tried in order:

| Path | Method | Speed |
|------|--------|-------|
| 1 | VUK lookup by disc hash in KEYDB.cfg | Instant |
| 2 | Media Key + Volume ID from KEYDB → derive VUK | Fast |
| 3 | Processing Keys + MKB → Media Key → VUK | Medium |
| 4 | Device Keys + MKB subset-difference tree → VUK | Slow |

The AACS handshake (`aacs/handshake`) performs ECDH key agreement over the
AACS 1.0 160-bit elliptic curve to obtain:
- **Volume ID** -- needed for VUK derivation (paths 2-4)
- **Read Data Key** -- needed for AACS 2.0 (UHD) bus decryption

Content decryption uses AES-128-CBC on 6144-byte aligned units. The
`ContentReader` handles this transparently. Streams that read sectors
(DiscStream, IsoStream) decrypt internally — the pipeline sees clean bytes.

---

## Error Codes

All errors carry a numeric code for programmatic handling. No user-facing text
is baked into the library.

| Range | Category | Examples |
|-------|----------|----------|
| E1xxx | Device errors | `DeviceNotFound`, `DevicePermission` |
| E2xxx | Profile errors | `UnsupportedDrive`, `ProfileNotFound`, `ProfileParse` |
| E3xxx | Unlock errors | `UnlockFailed`, `SignatureMismatch`, `NotUnlocked`, `NotCalibrated` |
| E4xxx | SCSI errors | `ScsiError`, `ScsiTimeout` |
| E5xxx | I/O errors | `IoError` (wraps `std::io::Error`) |
| E6xxx | Disc format errors | `DiscError` (UDF, MPLS, CLPI parse failures) |
| E7xxx | AACS errors | `AacsError` (key resolution, handshake, decryption) |
| E8xxx | KEYDB errors | `KeydbError` (download, parse, save) |

---

## Platform Support

| Platform | Transport | Status |
|----------|-----------|--------|
| Linux | async sg write/poll/read on `/dev/sg*` | Supported |
| macOS | IOKit SCSITask | Supported |
| Windows | SPTI (`IOCTL_SCSI_PASS_THROUGH_DIRECT`) | Supported |

The `ScsiTransport` trait abstracts the platform. Adding a new platform requires
implementing `execute()` for that OS and wiring it into `scsi::open()`.

---

## Chipset Support

| Chipset | Drives | Status |
|---------|--------|--------|
| MediaTek MT1959 | LG, ASUS, HP | Supported (bundled profiles) |
| Renesas RS8xxx/RS9xxx | Pioneer, some HL-DT-ST | Planned |

The `Platform` trait abstracts chipset-specific commands. Each chipset implements
handlers (unlock, config, register, calibrate, keepalive, status, probe,
read_sectors, timing). All handlers are accessed via SCSI READ BUFFER with
chipset-specific mode and buffer ID bytes.

---

## Build

```
cargo build --release
```

Produces a Rust library crate. The `libc` dependency is unix-only (gated).
All three platforms build and pass CI.
