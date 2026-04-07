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
   `DriveSession::open()` and `Disc::scan()`.

2. **No external files.** 206 drive profiles are compiled into the binary via
   `include_str!`. No configuration directory, no runtime file lookups for drive
   support.

3. **Transparent AACS.** The `ContentReader` decrypts on the fly when keys are
   available. Callers read cleartext sectors without knowing whether the disc
   was encrypted.

4. **Structured errors, no English.** Every error has a numeric code (E1000-E7000).
   The library never formats user-facing messages -- applications do that.

5. **Library-agnostic.** No concept of "supported" vs "unsupported" drives at a
   policy level. If a profile exists, the library uses it.

---

## Module Map

```
libfreemkv (lib.rs)
│
├── Drive Access
│   ├── drive         DriveSession — open, identify, unlock, read
│   ├── scsi          ScsiTransport trait + SG_IO implementation
│   ├── platform/     Platform trait — per-chipset command handlers
│   │   └── mt1959    MediaTek MT1959 driver (LG, ASUS, hp)
│   ├── profile       DriveProfile loading, matching, bundled JSON
│   ├── identity      DriveId from INQUIRY + GET_CONFIG 010C
│   └── speed         DriveSpeed enum, SET CD SPEED CDB builder
│
├── Disc Scanning
│   ├── disc          Disc::scan() — titles, streams, extents, AACS setup
│   ├── udf           UDF 2.50 filesystem reader (metadata partitions)
│   ├── mpls          MPLS playlist parser — clips, streams, STN table
│   ├── clpi          CLPI clip info parser — EP map, sector extents
│   └── jar           BD-J JAR label extraction (audio/subtitle names)
│
├── Encryption
│   ├── aacs          KEYDB parsing, VUK lookup, MKB processing, unit decryption
│   └── aacs_handshake  ECDH bus authentication, Volume ID, Read Data Key
│
└── error             Error enum with numeric codes E1000-E7000
```

---

## Drive Access Flow

```
DriveSession::open("/dev/sr0")
  │
  ├─ scsi::open()           Open /dev/sr0 via SG_IO
  ├─ DriveId::from_drive()  INQUIRY + GET_CONFIG 010C
  ├─ profile::find_by_drive_id()  Match against 206 bundled profiles
  ├─ Platform::new()        Instantiate chipset driver (Mt1959)
  └─ Platform::unlock()     Activate raw disc access mode
```

After open, the session provides:
- `read_sectors(lba, count, buf)` -- raw sector reads (through platform driver)
- `read_disc(lba, count, buf)` -- standard READ(10) for filesystem data
- `scsi_execute(cdb, dir, buf, timeout)` -- arbitrary SCSI commands
- `status()`, `calibrate()`, `read_config()`, `read_register()`

---

## Disc Scanning Flow

```
Disc::scan(&mut session, &ScanOptions)
  │
  ├─ READ CAPACITY          Get disc size in sectors
  ├─ udf::read_filesystem() Parse UDF 2.50 (AVDP → VDS → metadata → FSD → root)
  ├─ For each BDMV/PLAYLIST/*.mpls:
  │   ├─ mpls::parse()      Extract play items, STN streams
  │   └─ For each clip:
  │       └─ clpi::parse()  EP map → sector extents for the clip's time range
  ├─ Detect AACS            Check for /AACS directory on disc
  └─ Disc::setup_aacs()     Handshake + KEYDB → VUK → unit keys (if encrypted)
```

The result is a `Disc` with:
- `titles: Vec<Title>` -- sorted by duration, each with streams and sector extents
- `aacs: Option<AacsState>` -- decryption keys if available
- `encrypted: bool` -- whether the disc uses AACS

---

## AACS Decryption

Four key resolution paths, tried in order:

| Path | Method | Speed |
|------|--------|-------|
| 1 | VUK lookup by disc hash in KEYDB.cfg | Instant |
| 2 | Media Key + Volume ID from KEYDB → derive VUK | Fast |
| 3 | Processing Keys + MKB → Media Key → VUK | Medium |
| 4 | Device Keys + MKB subset-difference tree → VUK | Slow |

The AACS handshake (`aacs_handshake`) performs ECDH key agreement over the
AACS 1.0 160-bit elliptic curve to obtain:
- **Volume ID** -- needed for VUK derivation (paths 2-4)
- **Read Data Key** -- needed for AACS 2.0 (UHD) bus decryption

Content decryption uses AES-128-CBC on 6144-byte aligned units. The
`ContentReader` handles this transparently.

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

---

## Platform Support

| Platform | Transport | Status |
|----------|-----------|--------|
| Linux | SG_IO ioctl on `/dev/sr*` | Implemented |
| macOS | IOKit SCSI passthrough | Planned |
| Windows | SPTI (`IOCTL_SCSI_PASS_THROUGH_DIRECT`) | Planned |

The `ScsiTransport` trait abstracts the platform. Adding a new platform requires
implementing `execute()` for that OS and wiring it into `scsi::open()`.

---

## Chipset Support

| Chipset | Drives | Status |
|---------|--------|--------|
| MediaTek MT1959 | LG, ASUS, hp | Implemented (206 profiles) |
| Renesas RS8xxx/RS9xxx | Pioneer, some HL-DT-ST | Planned |

The `Platform` trait abstracts chipset-specific commands. Each chipset implements
10 handlers (unlock, config, register, calibrate, keepalive, status, probe,
read_sectors, timing). All handlers are accessed via SCSI READ BUFFER with
chipset-specific mode and buffer ID bytes.

---

## Build

```
cargo build --release
```

Linux builds produce a static library and two binaries (`freemkv-info`,
`freemkv-test`). The `libc` dependency is Linux-only. On non-Linux platforms,
the library compiles but `scsi::open()` returns a platform-not-supported error
until the IOKit/SPTI backends are implemented.
