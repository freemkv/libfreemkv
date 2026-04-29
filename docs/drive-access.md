# Drive Access and Unlock

Technical reference for how libfreemkv opens, identifies, unlocks, and reads
optical drives.

---

## Drive

`Drive` is the primary API. It owns the SCSI transport, the matched
drive profile, and the chipset-specific platform driver.

### Opening a Drive

```rust
let mut drive = Drive::open(Path::new("/dev/sg4"))?;
```

`open()` performs: open device → send INQUIRY → match profile → instantiate
platform driver. The drive is ready for `wait_ready()` and `init()`.

### Drive Operations

| Method | Description |
|--------|-------------|
| `wait_ready()` | Wait for disc insertion (30s timeout, TUR polling) |
| `init()` | Firmware upload + unlock + speed calibration |
| `probe_disc()` | Probe disc surface for optimal speeds |
| `read(lba, count, buf, recovery)` | Read sectors. Single-shot — no inline retries or reset. |
| `reset()` | Eject-cycle escape hatch. Caller-invoked only; not on the read path. |
| `lock_tray()` | Prevent tray ejection during rip |
| `unlock_tray()` | Allow tray ejection (also runs on Drop) |
| `eject()` | Eject disc tray |
| `drive_status()` | Query physical state (disc present, tray open, etc.) |
| `has_profile()` | Whether a bundled profile matched |
| `close()` | Consume Drive, cleanup (also runs via Drop) |

### init() Sequence

`init()` orchestrates the full drive unlock:

1. Platform driver `run_init()` — sends vendor-specific SCSI commands
2. If firmware upload needed: upload, wait 10s for drive reset, retry
3. Speed calibration after unlock
4. Max 3 attempts before giving up

### read() — single-shot

`Drive::read(lba, count, buf, recovery)` is the single read method. It issues
exactly one READ(10) CDB and returns the result. The `recovery` parameter only
selects the per-CDB timeout:

| `recovery` | Timeout  | Used by                                  |
|------------|----------|------------------------------------------|
| `false`    | 1.5 s    | `Disc::copy` fast skip-forward sweep, `DiscStream::fill_extents` |
| `true`     | 30 s     | `Disc::patch` multi-pass over the mapfile |

On any SCSI failure or timeout, `read` returns `Err(DiscRead)` immediately.
There are no inline retries, no SCSI reset, no Phase 1/2/3 escalation.

Recovery is layered above `Drive::read`:

- **Layer 1 — `Disc::patch`** loops over the ddrescue mapfile and re-issues
  `read(.., recovery=true)` against each non-`+` range.
- **Layer 3 — `DiscStream::fill_extents`** halves the request size on
  failure, retries at the same LBA, and probes back up on a clean-read
  streak.

Inline recovery (5× gentle retry → close + reset + reopen → 5× more) was
removed in 0.13.6. See the stop-wedge postmortem (2026-04-25)
for rationale: the inline reset wedged drive firmware on the LG BU40N (Initio
USB-SATA bridge) without ever recovering a sector. See
[`rip-recovery.md`](rip-recovery.md) for the full three-layer model.

---

## SCSI Transport

### Trait

```rust
pub trait ScsiTransport: Send {
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

### Platform Backends

| Platform | Implementation | Device |
|----------|---------------|--------|
| Linux | `SgIoTransport` — async `write`/`poll`/`read` on `/dev/sg*` | `/dev/sg*` |
| macOS | `MacScsiTransport` — IOKit SCSITask | IOKit service |
| Windows | `WindowsScsiTransport` — SPTI | `\\.\CdRomN` |

The Linux backend uses the sg driver's asynchronous interface: `write()` submits
the command, `poll()` waits with an enforceable wall-clock timeout, `read()`
retrieves the result. If `poll()` times out, the fd is abandoned (closed in a
background thread) and a fresh fd opened — the kernel's USB error recovery
cannot block us. Opens with `O_RDWR | O_NONBLOCK`.

On non-zero SCSI status, the transport parses sense key from the sense buffer
and returns `Error::ScsiError`.

`SgIoTransport::reset` (Linux) does pure userspace state cleanup: an open +
close pair to make the kernel cancel any SG_IO commands queued against a
previous fd, a 2 s sleep to let the kernel finish that cancellation, then a
fresh fd to send ALLOW MEDIUM REMOVAL to clear any stale tray lock. It does
NOT issue `SG_SCSI_RESET` or escalate via STOP+START UNIT. Both were tried
in 0.13.0–0.13.5 against the LG BU40N (Initio USB-SATA bridge); both failed
to recover wedged drives and made the wedge worse. The macOS reset (which
had been a no-op) was removed entirely in 0.13.6, and the top-level
`scsi::reset()` / `reset_with_timeout()` / `reset_blocking()` wrappers were
removed at the same time (no callers).

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

Profiles are JSON objects compiled into the binary (`profiles.json`).
Each profile contains:

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

Covers all LG, ASUS, and HP optical drives. Two sub-variants share identical
logic with different SCSI parameters:

| Variant | READ BUFFER mode | Buffer ID |
|---------|------------------|-----------|
| MT1959-A | 0x01 | 0x44 |
| MT1959-B | 0x02 | 0x77 |

The Platform trait maps to command handlers:

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
  playlists, and CLPI clip info are readable without unlock. Standard READ(10)
  works on any drive.

- **READ(10) fails for encrypted content sectors.** The drive firmware returns
  SCSI errors (sense key 0x05, illegal request) when an application attempts to
  read sectors containing encrypted m2ts content without prior AACS
  authentication via the bus key.

- **Raw mode bypasses firmware restrictions.** After unlock, the drive accepts
  READ(10) with the raw read flag (CDB byte 1 = 0x08) for all sectors,
  regardless of encryption status.

### AACS Before Unlock

AACS bus authentication uses standard MMC REPORT KEY / SEND KEY commands.
On some drives these must execute before unlock. The `Disc::scan()` handles
this internally — it manages the handshake/unlock ordering automatically.

---

## Speed Control

After `probe_disc()`, the platform driver maintains a speed lookup table
built by probing the disc surface. On each `read()` call, the driver:

1. Looks up the optimal speed for the target LBA.
2. Issues SET CD SPEED (0xBB) if the speed differs from current.
3. Performs the READ(10).

Available speeds:

| Format | Speeds |
|--------|--------|
| Blu-ray | 1x (4,500 KB/s) through 12x (54,000 KB/s) |
| DVD | 1x (1,385 KB/s) through 16x (22,160 KB/s) |
| Max | 0xFFFF (drive decides) |
