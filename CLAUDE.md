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

- **C shim** (`src/scsi/macos_shim.c`): `diskutil unmountDisk force` → find IOBDServices via `IOServiceMatching` → MMCDeviceInterface → SCSITaskDeviceInterface → `ObtainExclusiveAccess` → raw CDB dispatch via `CreateSCSITask` + `ExecuteTaskSync`.
- **Build** (`build.rs`): compiles shim via `cc` into static lib, linked by Cargo. NOT the `cc` crate (produces object code that breaks IOKit exclusive access).
- **Rust** (`src/scsi/macos.rs`): three FFI calls (`shim_open_exclusive`, `shim_close`, `shim_execute`). All CDBs go through single path — 1:1 with Linux SG_IO.
- **Key**: must find IOBDServices directly (not walk up from IOMedia). Must unmount before exclusive access. Must release service immediately after creating plugin.
- **IOBDServices parent chain**: IOMedia → IOBDBlockStorageDriver → IOBDServices → IOSCSIPeripheralDeviceType05. The block storage driver holds exclusive unless unmounted.
- **Test disc**: DUNE_PART_TWO UHD, `/dev/disk6`, 41288704 sectors.

## Bad-sector handling (BU40N + Initio INIC-1618L)

Three failure modes on this USB bridge:
1. **NOT READY** (sense_key=2, ASC=0x04, ASCQ=0x3E) — most common on BU40N for bad sectors. Pause 3s, retry up to 3x, then mark NonTrimmed.
2. **Transport failure** (status=0xFF) — bridge crash, auto-recovers ~15s. Aborts copy.
3. **INCOMPATIBLE FORMAT** (ASC=0x30) wedge — ALL sectors fail, requires power cycle.

### Adaptive probe algorithm (Pass 1 sweep)

When `skip_on_error=true` (multipass mode):
- Read each ECC block sequentially. On success, reset consecutive error counter.
- On error: zero-fill, mark NonTrimmed, increment consecutive error counter.
- After 4 consecutive errors: **probe** 1 sector at 256×batch (8 MB) ahead.
  - Probe succeeds: zero-fill the gap, mark it NonTrimmed, jump to probed position.
  - Probe fails: stay put, accumulate 4 more errors, probe again.
- Only transport failures (bridge crash) abort the pass.

Design rationale: consecutive errors (not total) so isolated scattered bad blocks don't trigger probes. The 8 MB probe distance clears typical ~30 MB bad zones in 2-3 probes. Gaps are zero-filled and marked NonTrimmed for patch passes to recover later.

## Public repo rules

- **No internal docs.** Audit reports, test plans, roadmaps, TODOs go in freemkv-private, never here.
- **No Co-Authored-By** in commit messages. One contributor: MattJackson.
- **No private references.** No Gitea URLs, no /data/code paths, no internal IPs in code.
