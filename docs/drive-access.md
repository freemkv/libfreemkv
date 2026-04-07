# Drive Access and Unlock

Technical reference for how libfreemkv opens, identifies, unlocks, and reads
optical drives.

---

## DriveSession

`DriveSession` is the primary API. It owns the SCSI transport, the matched
drive profile, and the chipset-specific platform driver.

### Opening a Drive

```rust
// Full open: identify → match profile → unlock
let mut session = DriveSession::open(Path::new("/dev/sr0"))?;

// No-unlock open: identify → match profile only
let mut session = DriveSession::open_no_unlock(Path::new("/dev/sr0"))?;

// Explicit profile (skip auto-detection)
let mut session = DriveSession::open_with_profile(Path::new("/dev/sr0"), profile)?;
```

**`open()`** performs the full sequence: open device, send INQUIRY, match
profile, instantiate platform driver, and unlock. Unlock failures are silently
ignored (unencrypted discs do not need it). After `open()`, both raw sector
reads and standard READ(10) work immediately.

**`open_no_unlock()`** skips the unlock step. This is required when AACS bus
authentication must happen before unlock. The handshake uses standard SCSI
commands that work without raw mode. After authentication completes, the caller
can invoke `session.unlock()` manually.

**`open_with_profile()`** bypasses profile auto-detection. Useful for testing
or when a custom profile is loaded from an external source.

### Session Operations

| Method | Description |
|--------|-------------|
| `unlock()` | Activate raw disc access mode via platform driver |
| `is_unlocked()` | Check if raw mode is active |
| `calibrate()` | Build speed lookup table for the current disc |
| `read_sectors(lba, count, buf)` | Raw sector read (requires unlock + calibrate) |
| `read_disc(lba, count, buf)` | Standard READ(10) with 5s timeout |
| `status()` | Query drive status and feature flags |
| `read_config()` | Read drive configuration block (1888 bytes) |
| `read_register(index)` | Read 16-byte hardware register |
| `probe(sub_cmd, addr, len)` | Generic READ BUFFER with caller parameters |
| `scsi_execute(cdb, dir, buf, timeout)` | Send an arbitrary SCSI CDB |

---

## SCSI Transport

### Trait

```rust
pub trait ScsiTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult>;
}
```

All drive communication goes through this trait. The library never opens file
descriptors or calls ioctls outside of a `ScsiTransport` implementation.

### Linux: SG_IO

The `SgIoTransport` implementation:

1. Opens the device path with `O_RDWR | O_NONBLOCK`.
2. Constructs an `sg_io_hdr` struct with the CDB, data buffer, and timeout.
3. Calls `ioctl(fd, SG_IO, &hdr)`.
4. Returns `ScsiResult` with status, bytes transferred, and sense data.

On non-zero SCSI status, the transport parses sense key, ASC, and ASCQ from the
sense buffer and returns `Error::ScsiError`.

### CDB Builders

The `scsi` module provides platform-agnostic CDB constructors:

| Function | CDB | Use |
|----------|-----|-----|
| `inquiry()` | INQUIRY (0x12) | Drive identification |
| `get_config_010c()` | GET CONFIGURATION (0x46) | Feature 010C firmware date |
| `build_read_buffer()` | READ BUFFER (0x3C) | All platform commands |
| `build_set_cd_speed()` | SET CD SPEED (0xBB) | Speed control |
| `build_read10_raw()` | READ(10) (0x28) with flag 0x08 | Raw sector reads |

---

## Drive Identification

`DriveId::from_drive()` sends two standard SCSI commands and extracts identity
fields:

| Field | Source | SCSI Reference |
|-------|--------|----------------|
| `vendor_id` | INQUIRY bytes [8:16] | SPC-4 section 6.4.2 |
| `product_id` | INQUIRY bytes [16:32] | SPC-4 section 6.4.2 |
| `product_revision` | INQUIRY bytes [32:36] | SPC-4 section 6.4.2 |
| `vendor_specific` | INQUIRY bytes [36:43] | SPC-4 section 6.4.2 |
| `firmware_date` | GET CONFIGURATION Feature 010C | MMC-6 section 5.3.10 |

The match key is `"VENDOR|PRODUCT|REVISION|VENDOR_SPECIFIC"`. Profile matching
tries all four fields first, then falls back to matching without the firmware
date for drives where Feature 010C is unavailable.

---

## Drive Profiles

Profiles are JSON objects compiled into the binary (`profiles.json`,
206 entries). Each profile contains:

| Field | Purpose |
|-------|---------|
| `vendor_id`, `product_revision`, `vendor_specific`, `firmware_date` | Matching fields |
| `chipset` | `"mediatek"` or `"renesas"` |
| `unlock_mode`, `unlock_buf_id` | READ BUFFER CDB parameters |
| `signature` | Expected 4-byte response signature |
| `unlock_cdb` | Pre-built unlock CDB (hex-encoded) |
| `register_offsets` | Offsets for hardware register reads |
| `capabilities` | Feature flags: `bd_raw_read`, `dvd_all_regions`, etc. |

Loading:

```rust
// Bundled (compiled-in) -- no file I/O
let profiles = profile::load_bundled()?;

// External file
let profiles = profile::load_all(Path::new("/path/to/profiles.json"))?;
```

---

## Chipsets

### MediaTek MT1959

Covers all LG, ASUS, and hp optical drives. Two sub-variants share identical
logic with different SCSI parameters:

| Variant | READ BUFFER mode | Buffer ID |
|---------|------------------|-----------|
| MT1959-A | 0x01 | 0x44 |
| MT1959-B | 0x02 | 0x77 |

The Platform trait maps to 10 command handlers:

| Handler | Function | Description |
|---------|----------|-------------|
| 0 | `unlock()` | Send READ BUFFER, verify signature + verification bytes |
| 1 | `read_config()` | Read 1888-byte configuration block + 4-byte status |
| 2-3 | `read_register()` | Read hardware registers at profile-specified offsets |
| 4 | `calibrate()` | Probe disc surface, build 64-entry speed table |
| 5 | `keepalive()` | Periodic session maintenance |
| 6 | `status()` | Query current mode and feature flags |
| 7 | `probe()` | Generic READ BUFFER with dynamic parameters |
| 8 | `read_sectors()` | Speed lookup + SET CD SPEED + READ(10) with flag 0x08 |
| 9 | `timing()` | Timing calibration |

### Renesas (Planned)

RS8xxx/RS9xxx chipsets used in Pioneer and some HL-DT-ST drives.
Currently returns `Error::UnsupportedDrive` when a Renesas profile is matched.

---

## Why Unlock Is Needed

Optical drive firmware restricts what applications can read from disc. Without
unlock:

- **READ(10) works for unencrypted filesystem data.** UDF structures, MPLS
  playlists, and CLPI clip info are readable without unlock. The `read_disc()`
  method uses standard READ(10) and works on any drive.

- **READ(10) fails for encrypted content sectors.** The drive firmware returns
  SCSI errors (sense key 0x05, illegal request) when an application attempts to
  read sectors containing encrypted m2ts content without prior AACS
  authentication via the bus key.

- **The kernel sr driver blocks block-device reads.** On Linux, the kernel's
  SCSI CD-ROM driver (`sr`) refuses to expose encrypted disc content through
  `/dev/sr0` as a block device. Even if you open the block device directly,
  reads to encrypted regions fail.

- **Raw mode bypasses firmware restrictions.** After unlock, the drive accepts
  READ(10) with the raw read flag (CDB byte 1 = 0x08) for all sectors,
  regardless of encryption status. This is how raw sector ripping works.

### open() vs open_no_unlock()

AACS bus authentication uses standard MMC REPORT KEY / SEND KEY commands.
These must execute before unlock because:

1. The AACS handshake establishes a bus key via ECDH.
2. The bus key encrypts the Volume ID and Read Data Key responses.
3. The Volume ID is needed to derive the Volume Unique Key (VUK).
4. The VUK is needed to decrypt unit keys from `Unit_Key_RO.inf`.

If `open()` unlocks first, some drives reject the subsequent AACS commands.
The correct sequence for encrypted discs is:

```rust
// 1. Open without unlock
let mut session = DriveSession::open_no_unlock(device)?;

// 2. AACS handshake (uses standard SCSI, no unlock needed)
let auth = aacs_handshake::aacs_authenticate(&mut session, &key, &cert)?;
let vid = aacs_handshake::read_volume_id(&mut session, &mut auth)?;

// 3. Now unlock for raw reads
session.unlock()?;
session.calibrate()?;

// 4. Read and decrypt content
session.read_sectors(lba, count, &mut buf)?;
```

In practice, `Disc::scan()` handles this internally. The default `open()` call
unlocks immediately and is correct for most use cases -- the scan re-opens a
second session with `open_no_unlock()` for the AACS handshake when needed.

---

## Speed Control

After `calibrate()`, the platform driver maintains a 64-entry speed lookup table
built by probing the disc surface. On each `read_sectors()` call, the driver:

1. Looks up the optimal speed for the target LBA in the table.
2. Issues SET CD SPEED (0xBB) if the speed differs from current.
3. Performs the READ(10).

Available speeds:

| Format | Speeds |
|--------|--------|
| Blu-ray | 1x (4,500 KB/s) through 12x (54,000 KB/s) |
| DVD | 1x (1,385 KB/s) through 16x (22,160 KB/s) |
| Max | 0xFFFF (drive decides) |
