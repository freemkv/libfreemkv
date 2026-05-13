# libfreemkv — Rules

## No English in library code

The library contains ZERO user-facing English text. All errors use numeric codes from `error.rs`. Applications (CLI, GUI, server) handle i18n.

- `io::Error::new(kind, "english string")` — NEVER. Use `Error::VariantName.into()`.
- If you need a new error, add a variant to `error.rs` with a code, not a string.
- Acceptable strings: debug/trace logging, test assertions, comments, data format strings (paths, codec IDs).
- `Error` implements `From<Error> for io::Error` — use `?` or `.into()` anywhere an `io::Error` is expected.

## Architecture

- **Streams are PES.** Every stream reads its format → PES frames out, or PES frames in → writes its format. One type per format.
- **Disc::copy() for sector dumps.** disc→ISO is NOT a stream. It's `Disc::copy()`.
- **DiscStream = any disc.** Physical drive or ISO file. Same type, different SectorReader.
- **No IOStream.** Deleted. No byte-level Read/Write on streams.
- **Streams don't know their size.** Progress/file_size is a CLI concern.
- **One method per action.** No `foo_with_X` variants. Use `Option<T>` params.
- **Streams impl Read only (conceptually).** No Seek, no File backing.
- **Functions return errors, only main() exits.** No `process::exit` in library code.

## Device rules

- Always use `/dev/sg*` not `/dev/sr*` for SCSI.
- `--raw` only skips decryption. Init/probe/speed still run.
- Each function does one thing. One runner orchestrates the sequence.

## macOS IOKit transport

The macOS SCSI transport uses exclusive IOKit access, not hybrid MMC+pread.

- **C shim** (`src/scsi/macos_shim.c`):
  - `shim_open_exclusive(bsd_name)`: `diskutil unmountDisk force` on target device only → find `IOBDServices` matching BSD name via IOKit registry walk → MMCDeviceInterface → SCSITaskDeviceInterface → `ObtainExclusiveAccess` → raw CDB dispatch.
  - `shim_list_drives()`: registry-based enumeration. Walks all `IOBDServices` entries, reads `"Device Characteristics"` for vendor/model/firmware, walks child chain to `IOMedia` for BSD name. Zero SCSI, zero exclusive access, zero unmounts.
  - `shim_execute()` / `shim_close()`: raw CDB dispatch and cleanup.
- **Build** (`build.rs`): compiles shim via `cc` into static lib, linked by Cargo. NOT the `cc` crate (produces object code that breaks IOKit exclusive access).
- **Rust** (`src/scsi/macos.rs`): FFI to `shim_open_exclusive`, `shim_close`, `shim_execute`, `shim_list_drives`. `list_drives()` uses registry-based enumeration. `MacScsiTransport::open()` uses exclusive access only when ripping a specific device.
- **IOBDServices parent chain**: IOSCSIPeripheralDeviceType05 → IOBDServices → IOBDBlockStorageDriver → IOMedia (has `"BSD Name"`). The shim walks this chain to match BSD name to IOBDServices.
- **IOKit lookup order**: (1) iterate all IOBDServices → match child IOMedia BSD name, (2) fallback: find IOMedia by BSD name → walk parent chain to IOBDServices, (3) fallback: first IOBDServices (single-drive systems).
- **Test disc**: DUNE_PART_TWO UHD, `/dev/disk6`, ~84.6 GB.

## Bad-sector handling (BU40N + Initio INIC-1618L)

Three failure modes on this USB bridge:
1. **NOT READY** (sense_key=2, ASC=0x04, ASCQ=0x3E) — most common on BU40N for bad sectors. Pause 3s, retry up to 3x, then mark NonTrimmed.
2. **Transport failure** (status=0xFF) — bridge crash, auto-recovers ~15s. Aborts copy.
3. **INCOMPATIBLE FORMAT** (ASC=0x30) wedge — ALL sectors fail, requires power cycle.

### Damage-jump algorithm (Pass 1 sweep)

When `skip_on_error=true` (multipass mode):
- Read each ECC block sequentially. Track a sliding window of the last 16 ECC block results.
- On error: zero-fill, mark NonTrimmed, push `false` to window.
- On success: write data, mark Finished, push `true` to window. Track consecutive good count.
- When ≥12% of the 16-block window are failures → **jump** ahead by `JUMP_BASE_SECTORS (1024) × batch × multiplier` sectors. For UHD encrypted ECC (batch=32) that's a 64 MiB base jump. Zero-fill the gap as NonTrimmed. Double the multiplier (64→128→256→512 MiB...) up to `MAX_JUMP_MULTIPLIER=64` (4 GiB cap). Plus a separate wedge-skip path of `WEDGE_JUMP_SECTORS=524288` (1 GiB) for HARDWARE_ERROR / ILLEGAL_REQUEST senses, capped at 16 consecutive wedges.
- When 16 consecutive good reads → reset multiplier to 1, restore max read speed.
- Only transport failures (bridge crash) abort the pass.

Tuning knobs: `DAMAGE_WINDOW=16` and `DAMAGE_THRESHOLD_PCT=12%`. Calibrated from live BU40N data: old 50/25% was too diluted by good reads between sparse failures; 16/12% triggers on the 2nd scattered failure (2/16 = 12.5% ≥ 12%).

### Patch (Pass N) — `disc/mod.rs:1910`

- Default: **reverse** mode. Walks bad ranges from highest LBA to lowest, and within each range from end to start. Rationale: sweep jumps forward with escalating gaps, so NonTrimmed ranges have good data at their tail (where the jump landed). Reverse hits good data first, converges on actual bad block boundaries.
- Single-sector reads with 60 s timeout (`READ_RECOVERY_TIMEOUT_MS`).
- NOT_READY (sense=2, ASC ∈ {0x02, 0x03, 0x04}): 15 s pause, retry without immediate Unreadable mark.
- Non-marginal SCSI sense → mark Unreadable and continue.
- Skip escalation: damage window 16, `PASSN_DAMAGE_THRESHOLD_PCT=6`, skip `PASSN_SKIP_SECTORS_BASE (32) << escalation` sectors capped at `PASSN_SKIP_SECTORS_CAP=4096`; `MAX_SKIPS_PER_RANGE=10`, then mark range Unreadable.
- Wedge exit: 50 consecutive failures **and** ≥ 2 ranges attempted (single-range stalls don't kill the pass).
- Whole-pass watchdog: `STALL_SECS = 3600` on `bytes_good`. Per-range watchdog: proportional `range_sectors × SECONDS_PER_SECTOR(25)`, capped at `RANGE_BUDGET_CAP_SECS=1800` (replaces the old flat 180s/range — tiny ranges got starved).

Constants live in `disc/patch.rs::Disc::patch` (PASSN_*, STALL_SECS, SECONDS_PER_SECTOR, RANGE_BUDGET_CAP_SECS, MAX_SKIPS_PER_RANGE). The full algorithm is documented in `freemkv-private/memory/project_recovery_v0_16.md`.

## Public repo rules

- **No internal docs.** Audit reports, test plans, roadmaps, TODOs go in freemkv-private, never here.
- **No Co-Authored-By** in commit messages. One contributor: MattJackson.
- **No private references.** No Gitea URLs, no /data/code paths, no internal IPs in code.
