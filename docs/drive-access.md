# Drive Access and Unlock

Technical reference for how libfreemkv opens, identifies, unlocks, and reads
optical drives.

---

## Drive

`Drive` is the primary API. It owns the SCSI transport and the drive
identity (`DriveId`); any drive-specific unlock logic lives behind the
pluggable [unlock seam](#drive-unlock-seam), not in `Drive` itself.

### Opening a Drive

```rust
let mut drive = Drive::open(Path::new("/dev/sg4"))?;
```

`open()` performs: open device → send INQUIRY → build `DriveId`. The drive
is ready for `wait_ready()` and `init()` (which routes through the unlock
seam).

### Drive Operations

| Method | Description |
|--------|-------------|
| `wait_ready()` | Wait for disc insertion (30s timeout, TUR polling) |
| `init()` | Route to the matching registered unlocker (if any), then prepare for reads |
| `probe_disc()` | Probe disc surface for optimal speeds |
| `read(lba, count, buf, recovery)` | Read sectors. Single-shot — no inline retries or reset. |
| `reset()` | Eject-cycle escape hatch. Caller-invoked only; not on the read path. |
| `lock_tray()` | Prevent tray ejection during rip |
| `unlock_tray()` | Allow tray ejection (also runs on Drop) |
| `eject()` | Eject disc tray |
| `drive_status()` | Query physical state (disc present, tray open, etc.) |
| `has_profile()` | Whether a registered unlocker matches this drive |
| `close()` | Consume Drive, cleanup (also runs via Drop) |

### init() Sequence

`init()` routes drive preparation through the unlock seam:

1. Walk the registered-unlocker registry; the first whose `matches()` is true
   is asked to `unlock_drive()` over the raw transport.
2. Whatever that unlocker needs (firmware upload, vendor handshakes, retries)
   is the unlocker's own business — libfreemkv only forwards the transport.
3. If no unlocker matches, the drive is left untouched and the library uses
   the host-certificate AACS handshake.

See [Drive Unlock Seam](#drive-unlock-seam) for the trait and registry.

### read() — single-shot

`Drive::read(lba, count, buf, recovery)` is the single read method. It issues
exactly one READ(10) CDB and returns the result. The `recovery` parameter only
selects the per-CDB timeout:

| `recovery` | Timeout  | Used by                                  |
|------------|----------|------------------------------------------|
| `false`    | 1.5 s    | `Disc::sweep` fast skip-forward pass, `DiscStream::fill_extents` |
| `true`     | 30 s     | `Disc::patch` retry pass over the mapfile |

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

The Linux backend uses the sg driver's asynchronous interface: `write()` submits
the command, `poll()` waits with an enforceable wall-clock timeout, `read()`
retrieves the result. If `poll()` times out, the fd is abandoned (closed in a
background thread) and a fresh fd opened — the kernel's USB error recovery
cannot block us. Opens with `O_RDWR | O_NONBLOCK`.

The macOS backend uses a C shim (`macos_shim.c`) for IOKit exclusive access.
The shim handles:
1. `shim_open_exclusive(bsd_name)` — unmounts the target device via `diskutil`,
   then walks the IOKit registry to find the `IOBDServices` matching the
   requested BSD name (IOBDServices → IOBDBlockStorageDriver → IOMedia → "BSD Name"),
   then creates MMCDeviceInterface → SCSITaskDeviceInterface → ObtainExclusiveAccess.
2. `shim_list_drives()` — registry-based enumeration with zero SCSI, zero exclusive
   access, zero unmounts. Reads IOBDServices "Device Characteristics" for
   vendor/model/firmware and child IOMedia "BSD Name" for the device path.
3. `shim_execute()` / `shim_close()` — raw CDB dispatch and cleanup.

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

## Drive Unlock Seam

libfreemkv ships **no firmware, no unlock CDBs, and no drive profiles.** It
knows only the *seam*, never the *mechanism*. The seam is the `Unlocker`
trait plus a small process-wide registry (`src/unlock.rs`):

```rust
pub trait Unlocker: Send + Sync {
    /// Stable, language-neutral identifier (logged).
    fn name(&self) -> &str;

    /// True if this unlocker handles the given drive.
    fn matches(&self, id: &DriveId) -> bool;

    /// Put the drive into extended-access mode. The one required capability.
    fn unlock_drive(&self, scsi: &mut dyn ScsiTransport, id: &DriveId) -> Result<()>;

    /// Read the disc Volume ID via the drive's OEM path. Default: no-op.
    fn read_volume_id(&self, _scsi: &mut dyn ScsiTransport, _id: &DriveId)
        -> Result<Option<[u8; 16]>> { Ok(None) }

    /// Raise the drive to its maximum read speed. Default: no-op.
    fn set_max_read_speed(&self, _scsi: &mut dyn ScsiTransport, _id: &DriveId)
        -> Result<()> { Ok(()) }
}
```

An unlocker is supplied by an **external crate** and registered once at
process start:

```rust
libfreemkv::register_unlocker(Box::new(some_unlocker::Plugin::new()));
```

The implementor owns everything about *how* a particular drive family is
driven — drive identification against its own profile database, firmware
upload, vendor CDBs, variant logic. libfreemkv only hands over the raw
`ScsiTransport` and the `DriveId`.

### Routing

At drive-prep the registry is walked in registration order; the first
unlocker whose `matches()` returns true is asked to `unlock_drive()` (and,
when needed, `read_volume_id()` / `set_max_read_speed()`). If no unlocker
matches, the drive is left untouched and the library falls back to the
standard host-certificate AACS handshake (the "OEM route"). The
`register_unlocker(...)` line is the entire plug: drop it (and the unlocker
crate) and libfreemkv still compiles and rips via the cert handshake.

Concrete unlockers — including the firmware-unlock profile databases,
variant logic, and vendor CDBs that used to live in-tree — are maintained
in the separate **[freemkv-unlock](https://github.com/freemkv/freemkv-unlock)**
repository, never here.

---

## Speed Control

A matching unlocker may raise the drive to its maximum read speed via
`set_max_read_speed()` (a no-op when no unlocker matches or the unlocker
declines). The library issues SET CD SPEED (0xBB) through the generic CDB
builder; the concrete speed policy lives in the unlocker.

Available speeds:

| Format | Speeds |
|--------|--------|
| Blu-ray | 1x (4,500 KB/s) through 12x (54,000 KB/s) |
| DVD | 1x (1,385 KB/s) through 16x (22,160 KB/s) |
| Max | 0xFFFF (drive decides) |
