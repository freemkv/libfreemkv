# Changelog

## 0.13.40 (2026-04-28)

### Pass 1 pure ECC-block sweep, transport-failure abort, mapfile-based recovery

This release reworks the sector-copy pipeline to handle unreliable USB-SATA
bridges (notably the Initio INIC-1618L) that crash on MEDIUM ERROR retries.

**Disc::copy() — Pass 1 (ECC-block sweep):**
- Reads `batch` sectors (default 32 = 1 BD ECC block = 64 KB).
- Success → mark Finished. MEDIUM ERROR → zero-fill, mark NonTrimmed, advance.
- Transport failure (host_status=7) → abort immediately, return error.
- No single-sector reads, no retry, no state machine in Pass 1.

**Disc::patch() — Pass 2+ (single-sector recovery):**
- Reads NonTrimmed sectors one at a time with pause between failures.
- Succeeds → mark Finished. Fails → mark Unreadable (or leave for next pass).
- Multi-pass: caller runs patch repeatedly until 0 recovered.

**CopyOptions simplified:**
- Removed `skip_forward`, `cautious_pause_ms`, `BPT1_EXIT_THRESHOLD`.
- Fields: `decrypt`, `resume`, `batch_sectors`, `skip_on_error`, `progress`, `halt`.

**Other fixes since 0.13.26:**
- `open()` just opens the device — no side effects. `drive_has_disc()` is a
  standalone TUR, not a probe sequence.
- `enumerate_sg_names()` skips unreadable `/sys` type files.
- SCSI sense data preserved in DiscRead errors (status + key + ASC/ASCQ).
- MapStats splits `bytes_pending` into `nontried` / `retryable`.
- Wallclock rip budget — halt after max(disc_runtime_secs, 3600).
- Patch instrumentation: counters for reads_ok/err, writes_ok/err, finished/unreadable.
- `as_encoded_bytes()` replaces `as_bytes()` for portable OsStr handling.
- Removed inline retry/reset from Drive::read — orchestration layer handles recovery.
- Removed all `freemkv-private` references from public code.

## 0.13.26 (2026-04-27)

### Extend DiscRead with SCSI status/sense for 30% wedge diagnostics

`Error::DiscRead` now carries `status` (SCSI status byte) and `sense`
(ScsiSense with key/asc/ascq). Previously this info was discarded,
showing only `E6000: {sector}`. Now shows:
- `E6000: {sector} 0x{status}/0x{sense_key}/0x{asc}`
- Enables recovery loop to distinguish recoverable errors from drive wedge
- Enables programmatic handling: `if status == 0xFF { reset } else { retry }`

### Error display shows up to 5 fields

Display format changed from `E{sector}` to `E{code}: sector status/key/asc`.

## 0.13.25 (2026-04-27)

### Drop dead `Drive::device_path_owned()`

The method was marked `// NOTE: Debug aid — remove after fd issue is
resolved` and the fd issue closed in 0.13.6. Use `device_path()` (which
returns `&str`) instead. Removing it clears a `cargo clippy -- -D
warnings` red on Linux CI that the Mac toolchain doesn't catch.

### Pre-commit gate uses CI's exact toolchain

`freemkv-private/scripts/precommit.sh` (new) runs `cargo +1.86 fmt
--check`, `cargo +1.86 clippy -- -D warnings`, and `cargo +1.86 test
--tests` across all 5 freemkv crates. Mirrors each repo's
`.github/workflows/ci.yml` step-for-step. Use it as a pre-commit hook
or run by hand before pushing — green here means green CI.

The Mac default toolchain is newer (1.94) and its clippy rejects
slightly different sets of lints than 1.86 — running locally without
pinning misses lints CI catches. The script forces 1.86 so drift
between local and CI ends.

## 0.13.24 (2026-04-27)

### MapStats: split `bytes_pending` into `bytes_nontried` + `bytes_retryable`

`MapStats.bytes_pending` aggregates `NonTried` (sectors Pass 1 hasn't
reached) + `NonTrimmed` + `NonScraped` (sectors flagged for Pass 2-N
retry). UIs that wanted a "MAYBE / will retry" bucket were stuck
showing the entire unread disc as "Maybe" at pct=0.

v0.13.24 keeps `bytes_pending` for back-compat and adds two granular
fields:

  - `bytes_nontried` — Pass 1 hasn't read these yet
  - `bytes_retryable` — `NonTrimmed + NonScraped`, Pass 2-N will retry

`bytes_pending == bytes_nontried + bytes_retryable` (invariant).

### cargo fmt cleanup

Picks up the `cargo fmt --check` lint failure that's been red on
`main` since v0.13.18 (long format-string layouts the local rustfmt
folded differently from CI's runner).

## 0.13.23 (2026-04-27)

### Stop discarding the drive's SCSI sense data

Through the entire 0.13.x line, every CHECK CONDITION reply from the
drive (the standard way SCSI reports a sector failure) was being
collapsed into a synthetic `status=0xFF, sense_key=0` "transport
wedge" sentinel and the real sense data was thrown away. Live tracing
on the BU40N reading Dune 2 on 2026-04-27 confirmed it: the drive was
returning `host_status=0, driver_status=8, status=2, exec_elapsed_ms=1416`
on every bad sector — a clean CHECK CONDITION carrying full sense
data — and the library was misclassifying it as a wedge and bailing.

Root cause: `scsi/linux.rs`'s wedge check was `host_status != 0 ||
driver_status != 0`. SG's `DRIVER_SENSE` bit (0x08) is set on every
CHECK CONDITION reply just to flag "sense buffer is populated" — it's
not a transport failure on its own. Pre-0.13.23 we conflated the two
and silently lost every drive-reported error reason. macOS and Windows
backends had the same shape: they extracted `sense_key` only, dropping
ASC/ASCQ.

### What 0.13.23 changes (API)

- **Linux**: mask `DRIVER_SENSE` before treating `driver_status` as a
  transport-layer failure. Real transport failures (`host_status != 0`
  or any non-SENSE bit set) still synthesise the `0xFF` sentinel.
- **`Error::ScsiError`** carries `sense: Option<ScsiSense>` instead of
  flat `sense_key`/`asc`/`ascq`. `sense=None` ⇔ transport failure (no
  SCSI status delivered). `Some(ScsiSense {…})` ⇔ drive replied with
  sense data. Removes the `0xFF`/`sense_key=0` magic-number coupling.
- **`ScsiSense`** is a public type with predicate methods on it —
  `is_marginal`, `is_medium_error`, `is_hardware_error`,
  `is_unit_attention`, `is_data_protect`, `is_not_ready`,
  `is_illegal_request`, `is_aborted_command`. Callers route on the
  structured fields rather than raw key comparisons.
- **`Error::scsi_sense()`** / **`Error::is_scsi_transport_failure()`** /
  **`Error::is_marginal_read()`** convenience predicates on `Error`.
  `is_marginal_read` is the high-level "should `Disc::copy` engage
  hysteresis on this error?" check.
- **SCSI protocol constants** (`SCSI_STATUS_GOOD`,
  `SCSI_STATUS_CHECK_CONDITION`, `SCSI_STATUS_TRANSPORT_FAILURE`,
  `SENSE_KEY_*`) moved from `error.rs` to `scsi/mod.rs` where they
  belong alongside `SCSI_INQUIRY`, `SCSI_READ_10`, etc.
- **macOS** + **Windows** backends parse the full sense triple too.
  Same code path on every platform — a regression in `parse_sense`
  would surface on all three OSes simultaneously.
- **`parse_sense`** replaces `parse_sense_key` (returns the full
  triple). Inline sense-format tests (descriptor 0x72/0x73 vs fixed
  0x70/0x71, short-buffer, VALID-bit masking, unknown response codes)
  now also exercise ASC/ASCQ extraction at the right offsets.

### Disc::copy + Disc::patch sense-aware dispatch

Both passes now bail immediately when a read fails with a sense class
that retry can't help (HARDWARE ERROR, DATA PROTECT, UNIT ATTENTION,
NOT READY, ILLEGAL REQUEST, real transport failure, kernel `IoError`)
rather than burning hysteresis cycles on a doomed loop. Marginal-read
sense (MEDIUM ERROR, ABORTED COMMAND, RECOVERED ERROR, NO SENSE)
engages hysteresis as before. New `phase=bail` trace event records
the bail reason.

`Disc::patch`'s `wedged_threshold` (50 consecutive failures) remains
as defense-in-depth for chains of marginal failures, but a single
non-marginal sense now short-circuits it.

### Behavioural impact

For damaged-disc rips on the BU40N this unblocks v0.13.22's
hysteresis: pre-fix, the misclassified "wedge" caused `Disc::copy` to
exit before hysteresis could engage, so `bytes_good` froze at the bad
zone. Post-fix the drive's CHECK CONDITION replies flow through the
normal path → hysteresis drops to bpt=1 → marginal sectors are
recovered or marked Unreadable. Calibration data
(`docs/audits/2026-04-26-bisect-on-fail-empirical-findings.md`) shows
~86 % of marginal-region sectors recover at bpt=1 on this drive.

## 0.13.22 (2026-04-26)

### Replace bisect-on-fail with hysteresis state machine (Block ↔ Single)

Live test on Dune 2 v0.13.21 showed bisect-on-fail recovered every
recoverable sector, but spent ~30 sec per damaged 60-block (paying a
~5 sec kernel timeout at every bisection level). Each level descended
log₂(60) ≈ 6 times on the failing branch.

Replaced with a two-state hysteresis machine in `Disc::copy`:

```
Block(batch):
  read(batch) ok    → write, advance, stay Block
  read(batch) fail  → switch to Single, retry SAME range at bpt=1

Single:
  read(1) ok    → write, consecutive_good++
                  if consecutive_good >= BPT1_EXIT_THRESHOLD:
                     switch to Block, reset counter
  read(1) fail  → mark NonTrimmed, consecutive_good = 0
```

`BPT1_EXIT_THRESHOLD = 10_000` sectors (= 20 MB of clean data).
Calibrated from the 2026-04-26 BU40N empirical run; tunable.

Per-block cost on a 60-sector damaged block with 1 bad sector:
- Bisect (v0.13.21): ~30 sec (5 s × 6 levels)
- Hysteresis (v0.13.22): ~10 sec (5 s bpt=batch fail + 59 × 1 ms good
  + 1 × 5 s bad)

Plus inside a damaged cluster spanning many 60-blocks, hysteresis
pays the bpt=batch fail cost ONCE on entry; bisection paid it every
60 sectors. For Dune 2's ~1248-sector boundary cluster that's ~21
fewer 5-sec waits = ~100 sec saved.

Telemetry: new `phase=mode_change` trace event with `from`, `to`,
`lba`, `consecutive_good`. Replaces v0.13.21's `phase=bisect`. The
v0.13.21 worklist DFS is gone — single iterative `for s in 0..count`
on the failure path.

Test rename:
`test_disc_copy_bisect_recovers_via_single_sector_reads` →
`test_disc_copy_hysteresis_recovers_via_single_sector_reads`. Same
synthetic BU40N-pattern reader; same 100% recovery expectation.

## 0.13.21 (2026-04-26)

### Fix: Disc::copy bisect-on-fail (replaces skip-forward)

Empirical live-hardware testing on the LG BU40N (see
`freemkv-private/docs/audits/2026-04-26-test-plan-audit.md` and the
TEST_PLAN.md run log) revealed that the drive often **fails
multi-sector READ commands** in damaged regions but **succeeds when
asked one sector at a time**. The old skip-forward strategy responded
to multi-sector failures by jumping up to 1 % of the disc forward,
marking everything in between as bad — losing **clean territory**
sandwiched between bad sectors.

`Disc::copy` now bisects on read failure: split the failed block in
half, retry each half, recurse down to single-sector reads. Sectors
the drive can read individually are recovered in Pass 1; only sectors
that fail at bpt=1 are marked NonTrimmed for the patch passes.

Empirical results on Dune 2 UHD on the BU40N:
- Old algorithm: 25 GB read in Pass 1, then ~6 GB skip-forwarded;
  retry passes failed to recover most of the skipped zone.
- New algorithm: ~99 % of disc recovered in Pass 1; only the truly
  unreadable cluster (~14 % of a 2 MB hot zone) marked NonTrimmed.

Implementation: stack-based DFS in the inner read loop. log₂(batch)
levels max — for the default 60-sector batch, 6 levels. Multi-pass
machinery is untouched: Pass 2 .. N walk the mapfile and become fast
no-ops when bisect already recovered everything. New integration test
`test_disc_copy_bisect_recovers_via_single_sector_reads` validates
the behavior against a synthetic BU40N-pattern reader.

### Fix: READ_TIMEOUT_MS bumped 1.5 s → 10 s (caller-side)

The 0.13.20 SCSI rewrite gave the kernel mid-layer the ability to run
its own ABORT/RESET escalation. But callers (`Drive::read` for the
fast path) still passed `timeout_ms=1500`. Cold-start seek on the
BU40N can take ~1.5 s, which means **normal reads were being
cancelled at the boundary**, triggering the kernel mid-layer's
escalation, which the Initio bridge couldn't drain — resulting in the
firmware-level wedge that only physical replug recovers.

Live-hardware probe data:
- Sustained sequential read: 3–7 ms
- Cold-start seek + read: up to ~1500 ms
- Successful ECC recovery: 1.6–2.6 s
- Confirmed unreadable: 3.6–8.8 s (kernel timeout)

10 s is calibrated to cover every legitimate read with margin while
still short-circuiting truly bad sectors before the kernel runs full
LUN/BUS/HOST reset. `READ_RECOVERY_TIMEOUT_MS` (60 s) unchanged.

## 0.13.20 (2026-04-26)

### Architecture: SCSI transport — sync blocking SG_IO

`scsi/linux.rs` rewritten from async `write/poll/read + 1.5 s timeout
+ close-on-timeout in bg thread` to a single synchronous blocking
`ioctl(fd, SG_IO, &hdr)`. The old pattern abandoned slow-but-alive
commands faster than the drive could drain its internal queue,
deepening the BU40N wedge. Per the audit at
`freemkv-private/docs/audits/2026-04-26-scsi-architecture-research.md`,
no reference project (MakeMKV / sg_dd / ddrescue) does what we did —
all use sync blocking SG_IO with 8-60 s timeouts and let the kernel's
mid-layer (`scsi_eh.rst`) run ABORT TASK / LUN RESET / BUS RESET /
HOST RESET escalation internally.

What changed:
- `SgIoTransport::execute()` is one syscall now. Caller-supplied
  `timeout_ms` is honored by the kernel, which does its own
  ABORT/RESET escalation if the device times out.
- Errors check `host_status` and `driver_status` (both 0xFF-synthesised
  for the caller) in addition to `status` — transport-level failures
  no longer slip through as Ok.
- Sense-key parser handles both descriptor format (0x72/0x73, key at
  byte 1) and fixed format (0x70/0x71, key at byte 2).
- Deleted the `fd_recovery: Arc<AtomicI32>` field, the bg close+open
  thread, and the stale-fd swap dance. `scsi/linux.rs` shrank from
  ~720 to ~520 lines.
- Module doc rewritten to reflect the new architecture.

### Architecture: parity strip on macOS + Windows

`scsi/macos.rs` and `scsi/windows.rs` had `try_recover()` —
userspace handle-recovery on task failure. Same anti-pattern as the
Linux fd-recovery dance, removed for the same reason: the kernel
mid-layer already runs its own escalation. Errors bubble up directly.

Cleanups:
- `MacScsiTransport`: `try_recover()` deleted, `bsd_name` field
  deleted (was only used by try_recover), fail-fast device_iface guard
  deleted (no longer null'd mid-session).
- `SptiTransport`: `try_recover()` deleted, `wide_path` field deleted,
  INVALID_HANDLE guard deleted.

### API cleanup: drop `Drive::reset` and `find_drives`

Two duplicates removed from the public surface:

- `Drive::reset()` — escalating recovery (STOP/START unit + eject +
  reinit). Per the audit, userspace shouldn't escalate; the kernel
  already does. Only one internal caller (`wait_ready` line 195),
  which now just keeps polling TUR for 60 iterations. No external
  consumer used it.
- `pub fn find_drives() -> Vec<Drive>` — opened N drives just to throw
  most away. Only caller was `find_drive()` itself, which now uses
  `discover_drives()` directly. No external consumer used it. For
  lightweight enumeration (UI sidebar etc.) use `scsi::list_drives()`.

`lib.rs` re-export of `find_drives` removed.

## 0.13.19 (2026-04-26 — held, never released)

Held in development; folded into 0.13.20.

## 0.13.18 (2026-04-26)

### Sync release — no functional changes

Bumped to satisfy the unified-versioning rule. Actual fix is in autorip
(`web.rs` two-bar UI — separates per-pass and total progress bars +
their own text rows so the rip dashboard is readable again).

## 0.13.17 (2026-04-26)

### Sync release — no functional changes

Bumped to satisfy the unified-versioning rule. Actual fix is in autorip
(hot-plug rescan in the drive poll loop — autorip now picks up
unplug/replug events without a container restart).

## 0.13.16 (2026-04-26)

### Architecture: single `Progress` trait + `PassProgress` struct

Pre-0.13.16 the rip API leaked internal mapfile concepts (`pos`,
`bytes_good`, `work_done`, `bytes_pending`, `Finished`/`NonTrimmed`)
into per-pass positional callbacks. Consumers reinvented the math each
time, and the v0.13.15 UI bug surfaced exactly because of this — autorip's
web JS computed `progress_pct` from `bytes_good` while the backend
computed from `pos`, silent drift, frozen UI bar.

This release replaces both `Disc::copy::on_progress` and
`Disc::patch::on_progress` `Fn(u64, u64, u64)` callbacks with a single
`Progress` trait and `PassProgress` struct (new `progress` module).

```rust
pub struct PassProgress {
    pub kind: PassKind,            // Sweep | Trim {reverse} | Scrape {reverse} | Mux
    pub work_done: u64,
    pub work_total: u64,
    pub bytes_good_total: u64,
    pub bytes_total_disc: u64,
}

pub trait Progress {
    fn report(&self, p: &PassProgress);
}

impl<F: Fn(&PassProgress)> Progress for F { ... }
```

Both `CopyOptions::on_progress` and `PatchOptions::on_progress` are
renamed to `progress: Option<&dyn Progress>`. Closure callers update
trivially; struct callers gain a clean named-field shape with no
positional-arg confusion.

`PassKind` carries the semantic (sweep vs trim vs scrape vs mux) so
consumers can label phases without reinventing detection logic. The
`Mux` variant is reserved for v0.13.17 when the mux pipeline emits
progress; not yet emitted by libfreemkv code.

`Disc::patch` reports `Trim {reverse}` for retry passes with
`block_sectors >= 2` and `Scrape {reverse}` when `block_sectors == 1`
(the per-sector final pass). Direction comes through `reverse: bool`.

## 0.13.15 (2026-04-26)

### Breaking: `on_progress` callback gains `pos` parameter

Both `CopyOptions::on_progress` and `PatchOptions::on_progress` now take
`Fn(bytes_good: u64, pos: u64, total_bytes: u64)`. The new `pos` parameter
is the current sweep / retry position. Pass 1 callers should display
`pos / total_bytes` for the "% swept" UI bar — `bytes_good` only counts
clean reads (Finished sectors) and freezes during skip-forward bad zones,
which made every previous version's UI look hung at the bad-zone boundary.
This was the v0.13.9 stall-guard origin bug.

Live trace from v0.13.14: Pass 1 hit a Dune 2 bad zone at 24 GB and
appeared "stuck" for 14 minutes per autorip's UI (`bytes_good = 23.97 GB`
unchanged). Disc trace events showed `pos` actually advanced from 25.8 GB
to 70 GB during that window — Pass 1 was 83 % through the disc, marking
the post-bad-zone NonTrimmed via skip-forward exactly as designed. The
display lied. Now consumers can show the truth.

### Feature: `PatchOptions::reverse` for reverse-direction retry passes

When set, `Disc::patch` walks bad ranges from highest LBA to lowest, and
within each range reads sectors back-to-front. Hypothesis (per the live
v0.13.14 test): drives that wedge after a forward read of a bad sector
read fine when approached from end-of-disc backward — most of the
post-bad-zone NonTrimmed range is actually clean data the drive could
have read on Pass 1 had it not been wedged. autorip alternates F/R
across retry passes (Pass 2 = reverse half-batch, Pass 3 = forward
quarter-batch, ...).

### Feature: `PatchOptions::wedged_threshold` early-exit

When > 0, `Disc::patch` exits early if it sees this many consecutive
read failures with zero successful reads in the same pass. Saves the
wallclock budget for productive grinding when the drive has clearly
wedged on the bad zone for this pass — a future pass with a different
direction or block size may still recover. Reported via new
`PatchResult::wedged_exit: bool`.

### Trace: `patch_start` and `patch_done` events

`freemkv::disc` target now emits `patch_start` (block_sectors, recovery,
reverse, wedged_threshold, num_ranges) and `patch_done`
(blocks_attempted, blocks_read_ok, blocks_read_failed, wedged_exit,
halted, bytes_recovered) at Disc::patch boundaries.

## 0.13.14 (2026-04-25)

### Sync release — no functional changes in libfreemkv

Bumped solely to satisfy the unified-versioning rule. The actual fix in
this release is in autorip: the tracing subscriber now enables
`freemkv::scsi=trace,freemkv::disc=trace` so the v0.13.13 instrumentation
events actually surface in `/api/debug`. Without that filter override the
trace events were silently dropped by the default `libfreemkv=warn` rule.

## 0.13.13 (2026-04-25)

### Telemetry: instrument the rip pipeline for in-flight diagnosis

v0.13.12 shipped Fix 1+2+4 + cross-platform parity but a live test on Dune 2
showed Pass 1 sat for 14 minutes with `bytes_good=0` while the inner loop
appeared to iterate (the throttled `on_progress` log fired every 78s). The
async fd_recovery design at §7 said each `execute()` call should bound at
~1.5 s on poll timeout, with subsequent calls returning `DeviceNotFound` in
microseconds until recovery completes. Observed reality contradicts that:
each iteration takes ~60 s, not microseconds. Without trace-level telemetry
at the SCSI + Disc::copy boundaries we can't diagnose where the time goes.

This release adds the telemetry. No behavior change; instrumentation only.

- New dep: `tracing = "0.1"`. Per CLAUDE.md, debug/trace logging is permitted
  in libfreemkv (the no-English rule applies to errors, not telemetry).
  Consumers (autorip) wire a tracing subscriber and pipe events into the
  JSONL debug log automatically.
- `SgIoTransport::execute` (Linux): trace events at every state transition
  (entry, recovery_swap_ok, recovery_pending, write_ok / write_err, poll_done,
  timeout_spawn_recovery, scsi_err, read_err, ok). Each event includes the
  opcode and elapsed timing. The bg recovery thread also traces close_ms +
  open_ms so we can see if the kernel is hanging close+open.
- `Disc::copy`: trace events at copy_start, outer_loop, region_enter, every
  100 inner-loop iterations (iter_progress with pos / region_end / skip_size /
  bytes_good / read_ok_count / read_err_count / last_read_ms /
  copy_elapsed_ms), and copy_done.
- All trace events use `target` strings `freemkv::scsi` and `freemkv::disc`
  so consumers can filter by subsystem.

### What this enables

- A live rip will now produce a SCSI event stream visible at
  `/api/debug?n=N&q=freemkv::scsi`. We can finally answer: "is the inner
  loop iterating slowly because each call is slow, or fast with the bg
  thread blocked?"
- `bg_recovery_done` events with `close_ms` / `open_ms` reveal whether the
  kernel really takes 60 s for close+open on a wedged Initio bridge.

## 0.13.12 (2026-04-25)

### Fix: delete stall guard from `Disc::copy` (RIP_DESIGN.md §6 Fix 1)

The v0.13.9 stall guard at `disc/mod.rs` exited Pass 1 early when
`bytes_good` was flat for `stall_secs` (default 120s). This violated the
ddrescue model: Pass 1 must sweep end-to-end, marking failed reads
NonTrimmed for Pass 2 retry. The guard caused Pass 1 to bail at 30% on
Dune 2 with 56 GB still NonTried, leaving Pass 2 nothing useful to do.

- Deleted the stall-guard state vars and the `if cur_good != ...
  break 'outer;` block.
- Deleted `CopyOptions::stall_secs` field — no longer wired.
- Replaced the broken regression test
  `test_disc_copy_stall_detection_triggers_skip_forward` with
  `test_disc_copy_completes_full_disc_with_failing_reader` (asserts Pass 1
  walks to end-of-disc with everything NonTrimmed when reads keep failing)
  and added `test_disc_copy_halts_promptly_on_failing_reader` (halt flag
  honored within 2s mid-skip-forward).

### Fix: async SCSI transport recovery (RIP_DESIGN.md §6 Fix 2 / §7)

`SgIoTransport::execute` (Linux) previously did close-in-background +
synchronous open-on-main-thread on poll timeout. The kernel serialized
the main-thread `open()` against the in-flight `close()` of the same
`/dev/sg*`, blocking the rip thread up to ~60s per timeout.

- Added `fd_recovery: Arc<AtomicI32>` field. On poll timeout, both
  `close(old_fd)` AND `open(new_fd)` run in a background thread; the new
  fd is published to `fd_recovery`. Returns Err immediately. Main thread
  is never blocked beyond the `poll()` budget (~1.5 s).
- Top of `execute()`: if `self.fd < 0`, swap from `fd_recovery`. If
  recovery is also pending, return `DeviceNotFound` and let the caller's
  retry loop come back later.
- Drop drains any pending `fd_recovery` so the fd doesn't leak.
- Stripped the v0.13.9 stall-guard narrative comment that justified
  the deleted behavior.

### Fix: cross-platform SCSI parity — Windows + macOS recovery (RIP_DESIGN.md §15.1)

Per the platform parity rule (no stubs), Windows and macOS now have the
same observable recovery contract as Linux:

- `SptiTransport` (Windows): added `try_recover()` that calls
  `CloseHandle` + `CreateFileW` synchronously after a failed
  `DeviceIoControl`. Stripped English error string ("run as
  administrator") from `open()`. Fixed the ms→s timeout truncation
  (1500ms now rounds up to 2s, was 1s).
- `MacScsiTransport` (macOS): added `try_recover()` that releases the
  IOKit interface (`RELEASE_EXCLUSIVE` + `com_release`) and re-acquires
  via the new `acquire_device_iface()` helper. Stores `bsd_name` so
  recovery can re-call `find_scsi_service`.
- All three platforms: top of `execute()` returns `DeviceNotFound`
  immediately if a prior `try_recover()` left the transport in an
  invalid state. Drop guards null'd-out interfaces.
- Send is auto-derived on all three (i32 fd / isize HANDLE / IOKit
  interface ref are Send-safe); explicit comments document the
  intentional implicit Send and the absence of Sync.

### Fix: instrument `Disc::patch` — diagnostic counters (RIP_DESIGN.md §6 Fix 4)

`PatchResult` now reports `blocks_attempted`, `blocks_read_ok`,
`blocks_read_failed`. Pass 2's "100 minutes recovered 0 bytes" mystery
(Dune 2) becomes diagnosable from these counters: distinguish "drive
returned Ok but write/record dropped data" from "every read was Err for
the entire range" without instrumenting from outside the lib.

### Fix: honor `PatchOptions::full_recovery`

The field was previously read into `let _ = opts.full_recovery;` and
ignored — `read_sectors(..., true)` was hardcoded. Now routed to
`read_sectors(..., opts.full_recovery)`. Behavior unchanged for
default callers (which pass `true`).

### Doc: `CopyOptions::batch_sectors` accuracy

Doc comment said "Defaults to 32 sectors (64 KB)". Updated to describe
the actual production path: callers should resolve via
`detect_max_batch_sectors(device_path)` (kernel-reported sysfs value,
typically 60 sectors / ~120 KB on the BU40N). The 32-sector internal
fallback is only reached when `batch_sectors=None AND skip_forward=true`.

## 0.13.11 (2026-04-25)

### Fix: revert SgIoTransport timeout path to keep transport alive

v0.13.10 changed `SgIoTransport::execute` to set `fd = -1` on a poll
timeout (no reopen on the main thread, since that would serialize
against the spawned close()). The intent was to escape the 60-s
blocking reopen.

The cost was too high: a single transient poll timeout permanently
killed the transport. Live test on Dune 2 (post-replug):
- Pass 1 ran for **45 ms** then returned with 0 GB good and 80 GB
  pending.
- The first SCSI READ timed out, fd went to -1, every subsequent
  read returned `DeviceNotFound` instantly, Disc::copy raced through
  the entire disc skip-forwarding in milliseconds.
- Pass 2 inherited the dead Drive and was equally useless.

Revert: spawn close + reopen on main thread (the v0.13.5/8
behavior). Yes the main-thread open() may block up to ~60 s while
the kernel completes the abandoned command — but the v0.13.9
`Disc::copy` stall guard already caps catastrophic stalls at 120 s
of `bytes_good` non-advance. Net: per-timeout cost is ~60 s, but
Pass 1 cleanly bails out within 120 s of any wedge, and Pass 2 has
a working Drive to retry NonTrimmed ranges with `recovery=true` +
30 s timeouts.

The integration test for the stall guard
(`test_disc_copy_stall_detection_triggers_skip_forward`) continues
to pass — the guard fires regardless of which transport-recovery
strategy is in play.

## 0.13.10 (2026-04-25)

### Version sync — no functional changes

Sync bump for the autorip-side fix in 0.13.10 (Pass 1 batch reporting).

## 0.13.9 (2026-04-25)

### Fix: Disc::copy silent stall + SgIoTransport reopen-after-timeout serialization

Two correlated fixes for a hang observed live on the LG BU40N during a
v0.13.8 rip of Dune: Part Two. At ~30 % progress through Pass 1
(disc → ISO), `bytes_good` froze for 10+ minutes with `errs=0`,
no error surfaced, drive not wedged.

Root cause: `SgIoTransport::execute` (linux.rs) attempted to recover from
a `poll()` timeout by spawning a background `close()` of the old fd and
opening a fresh `/dev/sg*` fd on the main thread. On Linux, opening the
SAME device while a prior fd is mid-close serializes via the kernel's
per-device state lock — so the fresh `open()` blocks for as long as the
close does (until the kernel completes the in-flight CDB). This undid
the userspace 1.5 s timeout: each timed-out read added 60+ s to the
next iteration. From `Disc::copy`'s perspective, reads kept returning
Err slowly, the skip-forward path advanced `pos` but never `bytes_good`.

Fixes:
- `SgIoTransport::execute` no longer reopens on timeout. Spawns the
  close, sets `self.fd = -1`, returns Err immediately. Subsequent
  calls fail with `DeviceNotFound` (already gated at line 248).
  Caller (Drive) is invalidated until reopened. Pass 2's
  `Disc::patch` would need a fresh Drive; that's a v0.14 follow-up.
- `Disc::copy` adds a stall guard. New `CopyOptions::stall_secs:
  Option<u64>` (default 120 s). If `bytes_good` doesn't advance for
  the threshold, breaks the outer loop with `complete: false,
  bytes_pending > 0` so the caller's retry path picks up.

Tests: new `test_disc_copy_stall_detection_triggers_skip_forward` in
`tests/integration_progress_and_halt.rs` proves the guard fires within
the configured threshold.

Other:
- Cosmetic: warning text "rip thread did not drain within 35s" updated
  to 60s (matches the v0.13.8 timeout bump).

## 0.13.8 (2026-04-25)

### Version sync — no functional changes

Sync bump for the ecosystem. 0.13.8 carries autorip-side fixes:
post-stop "error" leak (halt-aware Err handling in Pass 1/2+),
60 s drain timeout, and a structural spawn_rip_thread helper.

## 0.13.7 (2026-04-25)

### Version sync — no functional changes

Sync bump for the ecosystem. All four freemkv crates (libfreemkv,
freemkv CLI, bdemu, autorip) always share a version number; 0.13.7
carries an autorip-side fix (HTTP-spawned rip/scan threads now
register for stop-drain).

## 0.13.6 (2026-04-25)

### Inline retry/reset stripped from `Drive::read`; `BytesRead` now emitted

Two related changes that close the loop on the BU40N wedge work from
0.13.1–0.13.4 and on the long-standing autorip "0 KB/s, 0%" UI bug.

**`Drive::read` is now single-shot.** The phase 1 / 2 / 3 retry loop
(reset → reopen → repeat) inside `Drive::read` is gone (~80 lines
deleted). `recovery=true` only bumps the per-CDB timeout to 30 s;
`recovery=false` keeps the 1.5 s timeout. On a failed read the
function returns `Err(DiscRead)` immediately. Per the BU40N
post-mortem, every USB / SCSI reset path tested in 0.13.1–0.13.3
resets the bridge but not the drive firmware, and the inline
reset+reopen *was* the wedge primitive itself — issuing it from
inside `Drive::read` produced multi-minute hangs and made the wedge
class harder to surface to the user. The correct retry layer is
`Disc::patch`'s outer multi-pass loop, which is unaffected. A stuck
drive now surfaces as a clean `DiscRead` to the caller, who can
prompt physical replug.

**SCSI reset surface trimmed.** `SgIoTransport::reset` (Linux) drops
the `SG_SCSI_RESET` ioctl and the STOP / START UNIT escalation; it
keeps the kernel `SG_IO` state flush plus `ALLOW MEDIUM REMOVAL`.
`MacScsiTransport::reset` is removed entirely (was open + drop +
sleep, no SCSI). The top-level `scsi::reset` /
`scsi::reset_with_timeout` / `scsi::reset_blocking` family is
removed — no callers remain after the `Drive::read` strip.

**`EventKind::BytesRead` now emitted.** The variant was declared in
0.13.0 but never fired. `DiscStream::fill_extents` now emits
`BytesRead { bytes_read_total, total_extents_bytes }` after every
successful sector read, so consumers in direct (no-mapfile) mode can
drive a real-time progress bar without polling `output.bytes_written`.
Multi-pass mode continues to use `Disc::copy`'s `on_progress`
callback unchanged. Drives the autorip per-device live progress UI.

`Drive::checked_sleep` is removed (only used by the recovery loop);
`Drive::sleep_until_halted` is `#[cfg(test)]`-only; `Drive::emit` is
retained because `BytesRead` uses it.

### Tests
- New `tests/integration_progress_and_halt.rs` (5 tests): `BytesRead`
  emission, `Disc::copy` `on_progress` regression guard, halt aborts
  copy, Drop safety, `FileSectorReader` round-trip.
- 233 unit tests + 5 integration tests pass.

### Net diff
~80 lines deleted, ~20 added.

### Version sync
0.13.6 ecosystem release (libfreemkv + freemkv + bdemu + autorip all
on 0.13.6).

## 0.13.5 (2026-04-25)

### Version sync — no functional changes
Sync bump for the ecosystem. All four freemkv crates (libfreemkv,
freemkv CLI, bdemu, autorip) always share a version number; 0.13.5
carries autorip-side fixes (stop-is-reset, startup staging sweep).

## 0.13.4 (2026-04-25)

### Wedge recovery rolled back + sysfs identity fallback

**What changed.** The in-library USB / SCSI wedge-recovery escalation
added in 0.13.1 – 0.13.3 has been removed. `drive_has_disc` now returns
the raw TUR result (or the `0xFF` poll-timeout wedge error) directly to
the caller. `scsi::usb_reset()` / `usb_reset_with_timeout()` /
`DEFAULT_USB_RESET_TIMEOUT_SECS` and the per-platform
`SgIoTransport::usb_reset` / `MacScsiTransport::usb_reset` /
`SptiTransport::usb_reset` are gone. All three platform backends pass
transport errors through verbatim, keeping the public
`list_drives` + `drive_has_disc` contract symmetric
(Linux / macOS / Windows).

**Why.** Production testing against the LG BU40N USB BD-RE (the drive
that drove the whole 0.13.1–0.13.3 recovery push) showed:
- `SG_SCSI_RESET`, `STOP UNIT` + `START UNIT`, and `USBDEVFS_RESET`
  all succeed at the USB transport layer (kernel logs
  `usb 3-2: reset high-speed USB device`, device re-authorises).
- But the drive firmware *below* the USB bridge stays locked: no
  LUN enumerates on the fresh `scsi_host`, TUR never succeeds,
  `/dev/sg*` never reappears.
- Also tried (outside the lib): `/sys/bus/usb/devices/<port>/authorized`
  toggle, `usb-storage` driver unbind/rebind, forced SCSI host rescan.
  All same outcome.

Only physical unplug-replug (or host reboot) clears this wedge class.
The library was logging 2-minute-per-tick escalation cycles for nothing,
and consumers had no way to surface "drive needs physical intervention"
to users because the escalation was masking the real failure. Upper
layers (autorip, CLI) now see the wedge error directly and prompt the
user.

A breadcrumb in `scsi/linux.rs::drive_has_disc` catalogues every
recovery method tried and points to git tag `v0.13.3` for the full
implementation, in case a future hardware class is found where
USB-layer recovery actually works.

**New: sysfs-cached identity fallback (Linux).** `list_drives` now
populates empty INQUIRY vendor/model/firmware fields from
`/sys/class/scsi_generic/sgN/device/{vendor,model,rev}` — the kernel
runs its own INQUIRY at device probe time and stashes the answer there,
so even a mid-wedge INQUIRY still yields the UI a human-readable
identity. The drive surface on screen doesn't suddenly go blank the
moment the drive firmware locks up.

## 0.13.3 (2026-04-24)

### Bug fix — `drive_has_disc` wedge recovery was dead code for TUR errors

The wedge-signature predicate introduced in 0.13.2 gated on
`opcode == SCSI_INQUIRY (0x12)` — a holdover from when enumerate-time
INQUIRY was the only path wedges surfaced on. `drive_has_disc` issues
`TEST UNIT READY (0x00)`, so its wedge errors (`E4000: 0x00/0xff/0x00`)
never matched the predicate and the SCSI-reset + USB-reset escalation
never fired. Production result: the BU40N USB BD-RE stayed wedged
indefinitely, with autorip logging `recovery exhausted` on the raw
pass-through error while no recovery had actually been attempted.

Fix: drop the opcode constraint. Status byte `0xFF` is synthesised by
our own `execute()` path when `poll()` on the SG fd times out; it's
the ground-truth wedge marker regardless of which opcode was in flight.
Doc comments on `is_wedge_signature` and `WEDGE_STATUS_BYTE` updated
accordingly. Linux-only — macOS / Windows use sense-key-based wedge
detection and are unaffected.

## 0.13.2 (2026-04-24)

### Public discovery + presence APIs; SCSI/USB primitives no longer
### exposed to consumer crates

The autorip / freemkv-CLI side of the ecosystem was reimplementing
hardware discovery (sysfs walking, SCSI type-5 filtering, sg-path
construction) and SCSI recovery primitives in their own crates — a
direct violation of the architectural rule that ALL hardware-aware
code lives in libfreemkv. 0.13.2 closes that gap with two cheap public
probes that absorb everything consumers were doing themselves, plus
visibility tightening to make future violations a compile error.

#### New public APIs

- `pub struct DriveInfo { path, vendor, model, firmware }` — a single
  enumerated optical drive's identity. Returned by `list_drives()`,
  populated from a single SCSI INQUIRY at enumeration time. No
  firmware reset, no `init`.
- `pub fn list_drives() -> Vec<DriveInfo>` — one-shot enumeration
  across Linux/macOS/Windows. Linux walks `/sys/class/scsi_generic/`
  with the SCSI type-5 filter and `/dev/sg0..15` fallback; macOS
  walks `/dev/disk0..15` with the INQUIRY peripheral-type-5 filter;
  Windows iterates `CdRom0..15`. Cheap (~10 ms / drive); cache the
  result and refresh on udev events.
- `pub fn drive_has_disc(path: &Path) -> Result<bool>` — single TEST
  UNIT READY. Returns `Ok(true)` when ready / `Ok(false)` on sense-key 2
  ("medium not present") / `Err` only after recovery has been exhausted.
  **Internal wedge recovery is hidden from callers** — when the kernel
  returns the wedge-signature pattern (status 0xFF, no sense), this
  function transparently escalates: SCSI bus reset → if still wedged →
  USB device reset → retry TUR. Consumers never see the escalation.

#### USB-layer reset, multi-platform

`USBDEVFS_RESET` (Linux) is the only thing that recovers a kernel-
level USB Mass Storage wedge — software equivalent of unplug-replug.
Now wired for all three OSes:

- **Linux**: `USBDEVFS_RESET` ioctl on `/dev/bus/usb/BBB/DDD`. Resolves
  sg → USB device via sysfs walk (`busnum`/`devnum` parents).
- **macOS**: `IOUSBDeviceInterface::ResetDevice()`. Walks IORegistry
  parents from the SCSI service to the USB device, queries the IOKit
  USB plugin, calls ResetDevice.
- **Windows**: existing `IOCTL_STORAGE_RESET_DEVICE` covers both SCSI
  and USB layers via storport, so `usb_reset` returns `DeviceNotFound`
  by design — the recovery escalation in `drive_has_disc` falls
  through cleanly. (See the `windows::usb_reset` doc comment for
  why a separate cycle-port IOCTL isn't needed on Windows.)

All wrapped in a thread + `mpsc::recv_timeout` so a kernel ioctl that
hangs forever can't lock up the caller (the inner thread leaks one OS
thread per hard wedge — acceptable for a daemon that recovers vs. one
that wedges the whole poll loop).

#### Visibility tightening (architectural enforcement)

These were `pub` in 0.13.1; consumer crates could (and did) call them
directly, leaking SCSI knowledge across the lib boundary:

- `scsi::reset` → `pub(crate)`
- `scsi::reset_with_timeout` → `pub(crate)`
- `scsi::usb_reset` → `pub(crate)`
- `scsi::usb_reset_with_timeout` → `pub(crate)`
- `DEFAULT_RESET_TIMEOUT_SECS` / `DEFAULT_USB_RESET_TIMEOUT_SECS` →
  `pub(crate)`

Consumers now reach recovery exclusively through `drive_has_disc`,
which folds the escalation in. **Compile-time guarantee** that no
future autorip/CLI/bdemu commit can reintroduce direct SCSI access.

#### Why this design

`Drive::open(path)` runs a ~2 s firmware-reset preamble + identify
sequence; suitable for ripping but wasteful for a poll loop probing
"is there a disc?". Pre-0.13.2 autorip called `Drive::open` 4 × every
5 s = ~17 000 speculative SCSI sessions/day, hammering the drives
between actual rips. The wedge in production at 23:51 UTC was
triggered by exactly this hot-loop pattern. With `drive_has_disc`,
the same poll cadence costs ~50 ms / drive (one TUR) — 40× cheaper
and side-effect-free on a healthy drive.

#### Tests

- 233 lib tests pass (no change in count; APIs covered indirectly via
  the existing transport tests + a new `device_key` test on the
  autorip side).
- `cargo clippy --all-targets -D warnings` clean across Linux/macOS.

## 0.13.1 (2026-04-24)

### `scsi::reset()` now has a hard wallclock timeout

Production incident on a wedged BU40N USB drive: autorip's poll loop
called `scsi::reset()` and the call hung for 60+ seconds before the
operator manually intervened. Root cause: the Linux `SG_SCSI_RESET`
ioctl can block indefinitely when the kernel SCSI subsystem is waiting
on a bus-wedged device that will never ack — there's no kernel-side
timeout on this ioctl. Without an outer wallclock bound the caller's
thread is stuck in the kernel until the device unwedges (which, for a
permanently-dead USB target, may be never).

`scsi::reset()` is now a wrapper that runs the platform-specific reset
on a detached worker thread and bounds the caller's wait via
`mpsc::recv_timeout(DEFAULT_RESET_TIMEOUT_SECS)` (30 s). Returns
`DeviceResetFailed` on timeout. The worker thread keeps running until
the kernel eventually unblocks (we can't cancel a Linux ioctl from
userspace) — this leaks one OS thread per hard wedge, an acceptable
cost for a daemon that recovers vs. one that hangs.

- New `pub const DEFAULT_RESET_TIMEOUT_SECS: u64 = 30;`
- New `pub fn reset_with_timeout(device, Duration) -> Result<()>` for
  callers that want a different bound.
- Existing `pub fn reset(device) -> Result<()>` keeps the same
  signature; behaviour change is the timeout, not the API.

### Follow-up flagged

`SG_SCSI_RESET` only resets at the SCSI layer. For USB-attached drives
(the BU40N case), the wedge is often in the USB Mass Storage layer
*below* SCSI — `SG_SCSI_RESET` doesn't help. The proper escalation is
`USBDEVFS_RESET` (the `usbreset.c` ioctl), which re-enumerates the
device at the USB layer. Tracked for 0.13.2: a `scsi::usb_reset(path)`
that resolves sg → USB device and issues `USBDEVFS_RESET`. That would
have recovered tonight's BU40N without operator intervention.

## 0.13.0 (2026-04-24)

### Zero English in library — typed variants for every error path

Audit pass against the `CLAUDE.md` rule (no English text in library code).
Found nine call sites that violated the contract by stuffing English into
`io::Error::new(kind, "…")` or by abusing `Error::DeviceNotFound { path }`
as a free-form description field. Each is now a typed variant with
structured fields; the CLI / autorip translates to localized text.

New `Error` variants and codes:

- `ScsiInterfaceUnavailable { path }` — `E1004` (macOS
  `SCSITaskDeviceInterface` couldn't be obtained)
- `DeviceLocked { path, kr }` — `E1005` (replaces an English
  "exclusive access denied. Try: diskutil unmountDisk" message)
- `IoKitPluginFailed { path, kr }` — `E1006`
- `UnsupportedPlatform { target }` — `E2003` (built on an OS without an
  SCSI backend)
- `PlatformNotImplemented { platform }` — `E2004` (replaces the
  `product_revision: "Renesas not yet implemented"` string-stuffing in
  `drive::mod`)
- `MapfileInvalid { kind }` — `E6011` (ddrescue mapfile parse, with a
  stable `&'static str` kind: `"status_char"` or `"hex"`)
- `DiscUrlNotDirect` — `E9009` (replaces the full English sentence
  `"Use Drive::open() + Disc::scan() + DiscStream::new() for disc sources"`
  that `mux::input(disc://…)` returned to callers)

Migrated call sites:

- `mux/resolve.rs` — disc URL → `DiscUrlNotDirect`; the four
  `format!("m2ts://…")` / `format!("mkv://…")` IO error wraps now
  propagate the inner `io::Error` unchanged (the URL-prefix wrap added
  no semantic information).
- `mux/iso.rs` — same `format!("iso://…")` wrap dropped.
- `sector.rs` — image-too-large now uses the existing `IsoTooLarge`
  variant instead of `format!("…image too large, max ~8 TB")`.
- `disc/mapfile.rs` — bad-status-char and bad-hex parser errors now use
  `MapfileInvalid { kind }`.
- `scsi/mod.rs` — unsupported-platform path uses `UnsupportedPlatform`.
- `scsi/macos.rs` — IOKit plugin failure → `IoKitPluginFailed`,
  `SCSITaskDeviceInterface` missing → `ScsiInterfaceUnavailable`,
  exclusive-access denied → `DeviceLocked` with the IOReturn code as a
  structured `kr` field. The `find_scsi_service` four-stage failure path
  is now a single `DeviceNotFound { path }` (none of the prior
  per-stage English descriptions were user-actionable individually).
- `drive/mod.rs` — Renesas platform → `PlatformNotImplemented`.

### Stripped English from `labels` module

`labels::apply()` previously pushed `"Commentary"`, `"Descriptive Audio"`,
`"Score"`, `"IME"`, and `" (Secondary)"` directly into
`AudioStream.label`. Those English strings then leaked into MKV titles
and into autorip's UI. The data was already structured upstream
(`LabelPurpose` enum on `StreamLabel`); the lib was downcasting it for
the caller's convenience.

- `AudioStream` gains `purpose: LabelPurpose` (re-exported from
  `crate::disc` next to the struct, alongside `LabelQualifier`).
- `SubtitleStream` gains `qualifier: LabelQualifier`.
- `apply()` writes structured fields, never English. `label` keeps
  codec-formatting only (`"Dolby TrueHD 5.1"`).
- `generate_audio_label` drops the `" (Secondary)"` suffix; `secondary`
  is already a `bool` field, callers render it.
- `generate_video_label` drops the `"Secondary Video"` fallback.
  `"Dolby Vision EL"` kept (brand identifier, not translatable).

### API hygiene

- **mux module visibility tightened**. `pub mod ebml`, `m2ts`, `mkv`,
  `network`, `null`, `ps`, `stdio`, `ts`, `tsmux` are now `pub(crate)`.
  Their *types* are still re-exported from `lib.rs` — the modules
  themselves were leaking low-level EBML primitives, TS muxer
  internals, and network/stdio implementations that no external caller
  used. `mux::codec`, `mux::disc`, `mux::iso`, `mux::resolve`, and
  `mux::meta` stay public (genuine APIs).
- **Stream trait rustdoc**. The keystone PES `Stream` trait (in
  `pes.rs`) had per-method docs but no trait-level doc. Now explains
  read-vs-write split, error contracts, the role of `info()` /
  `codec_private()` / `headers_ready()`.
- **lib.rs re-export sections**. Eight grouped sections with a
  paragraph each (Drive lifecycle, Errors, Decryption, Disc structure,
  Streams, Lower-level surfaces) so `cargo doc` tells callers when to
  reach for what.
- **Dropped `ScanOptions::with_keydb()`**. The `_with_X` constructor
  pattern was banned by `CLAUDE.md` (one method per action). Use the
  struct literal: `ScanOptions { keydb_path: Some(p.into()) }`. Five
  external call sites (autorip ×3, freemkv CLI ×3) and three test
  fixtures migrated.
- **`pid_index` allocation documented**. The `TsDemuxer::new` flat
  lookup table was flagged by audit as "unbounded for adversarial
  PIDs"; on closer reading it's bounded by `u16::MAX × 2 bytes ≈ 128 KB`.
  Doc comment now states the bound explicitly so future contributors
  don't re-flag it.

### Dead-code sweep

Pre-PES-rewrite leftovers that were `pub` but unreachable:

- Deleted `mux/lookahead.rs` entirely (orphan file — never had a `mod`
  declaration; only used by its own tests).
- Deleted `mux/tsreader.rs` (`TsDemuxReader` struct + four methods,
  used nowhere).
- Deleted `mux::ebml::write_int`, `read_vint`, `SEEK_HEAD`, `SEEK`,
  `SEEK_ID`, `SEEK_POSITION` (unused).
- Deleted `mux::ts::scan_first_pts`, `scan_last_pts`, `scan_duration`,
  `SCAN_HEAD_SIZE`, `SCAN_TAIL_SIZE`, `take_remainder`, `set_remainder`
  (unused since the v0.10 PES rewrite).
- Deleted `MkvMuxer::codec_private_slots` /
  `codec_private_filled` fields and `fill_codec_private` method —
  deferred-codecPrivate path was never exercised once codec_privates
  flowed through `DiscTitle`.

`cargo clippy --all-targets -- -D warnings` is clean.

### Tests

- New `error::tests` module — code distinctness, Display has no English
  words, `io::ErrorKind` mapping for every new variant.
- 233 lib tests (was 230), all green.

### Breaking changes

Source-compatible for callers who use `Error` opaquely (handle
`Result<T, Error>` and `error.code()` only). The following are breaking:

- `ScanOptions::with_keydb()` removed — use struct literal.
- `mux::ebml`, `mux::mkv`, `mux::ts`, etc. modules no longer accessible
  externally — use the re-exported types from the crate root instead.
- `AudioStream` and `SubtitleStream` gained required fields (`purpose`,
  `qualifier`). Construction-by-struct-literal must include them.
- `Error::UnsupportedDrive { product_revision: "Renesas not yet
  implemented" }` no longer produced — match `PlatformNotImplemented`.

### Magic-number policy

Per the v0.13 audit directive: new code in this release uses named
documented constants (e.g. `POLL_INTERVAL_SECS` in autorip, the wedge
signature literals in `ripper.rs`, the `BD_TS_PID_SPACE` floor in
`ts.rs`'s table allocation). A comprehensive retrofit of pre-existing
magic numbers across the older codebase is queued as follow-up work for
0.13.1+ — too large to absorb into this release without scope creep.

## 0.12.0 (2026-04-24)

### Rust 2024 edition migration
- Bumped `edition = "2024"`. Required code changes:
  - FFI declarations in `src/scsi/macos.rs` wrapped in `unsafe extern "C" { … }` per the 2024 FFI safety rules.
  - `unsafe_op_in_unsafe_fn` lint: `vtable_fn()` body now has an explicit `unsafe { … }` block rather than relying on implicit unsafe of the containing `unsafe fn`.
  - Match-ergonomics: removed redundant `ref`/`ref mut` bindings in `mux/meta.rs`, `mux/mkvstream.rs`, `mux/network.rs`, `mux/stdio.rs` — 2024 tightens "cannot explicitly borrow within an implicitly-borrowing pattern."
- No behavior change. MSRV stays at 1.86.

### Minor / version sync
- Part of the 0.12.0 ecosystem release. The autorip-side fixes (progress regressions, UI redesign, regression-guard tests) drove the minor bump.

## 0.11.22 (2026-04-24)

### Version sync — no functional changes
Part of the 0.11.22 ecosystem release. autorip 0.11.22 ships full multi-pass UI (bad-range viz, live mapfile stats, Recovery settings); the library API is unchanged from 0.11.21.

## 0.11.21 (2026-04-24)

### Multi-pass rip architecture — disc → ISO → patch → ISO

New primitives for two-stage rip: fast forward pass with zero-fill on failures, then targeted retries of bad ranges. Keeps the library API stream-based; the multi-pass model lives entirely in caller-orchestrated function composition.

- **New `Disc::copy(reader, path, &CopyOptions)`** replaces the positional-arg version. Always produces a ddrescue-format mapfile at `path + ".mapfile"` as a side-effect. With `skip_on_error=true` + `skip_forward=true`, does ddrescue-style fast sweep: 64 KB block reads, exponential skip-forward (256 KB → cap at 1% of disc) on failure, zero-fill bad blocks, record ranges in the mapfile. With defaults (both false), matches pre-0.11.21 behavior — uses drive-level recovery, aborts on bad sector. Mapfile is produced either way.
- **New `Disc::patch(reader, path, &PatchOptions)`** — idempotent retry pass. Reads the mapfile, re-reads every non-`+` range with full drive recovery enabled, writes successful bytes back into the ISO at exact offsets, updates mapfile. Call N times for N retry attempts.
- **New `disc::mapfile` module** — ddrescue-compatible plain-text format. Crash-safe (flushes on every `record()`), greppable, human-editable, tool-interoperable. Status chars match ddrescue: `?` non-tried · `*` non-trimmed · `/` non-scraped · `-` unreadable · `+` finished.
- **Re-exports:** `FileSectorReader` from the crate root for ISO readers.

### Breaking changes
- `Disc::copy`'s signature changes from positional args (`decrypt, resume, batch, on_progress`) to `CopyOptions`. Previous callers must migrate. `freemkv` CLI updated in lockstep.

### Version sync
- Part of the 0.11.21 ecosystem release (libfreemkv + freemkv + bdemu + autorip all on 0.11.21).

## 0.11.18 (2026-04-24)

### DiscStream halt flag — Stop works during dense bad-sector regions

`DiscStream::fill_extents` loops internally when the demuxer hasn't accumulated enough data to emit a PES frame — during a dense bad-sector run, that loop can spend many minutes shrinking batch sizes and zero-filling sectors without ever returning to the outer read() call. Without an internal halt check, the caller's Stop request goes unserviced until the demuxer eventually emits a frame, which may be very far away.

- **`DiscStream::set_halt(Arc<AtomicBool>)`** — share a halt flag with the stream. Typically wired to `Drive::halt_flag()` so Stop propagates across both the drive's recovery phases and the stream's sector processing.
- **`fill_extents()` checks the halt flag** at the top of every retry iteration (before each attempt at every size level). Raising the flag aborts within one read round-trip — at most the current SCSI command's timeout.
- Returns `Err(Error::Halted)` (E6010) so the outer rip pipeline terminates cleanly.

No behavior change for callers that don't call `set_halt`. Unblocks the architectural fix for the "Stop doesn't stop" bug observed on a damaged UHD disc where the stream was stuck in a 12+ hour bad-sector grind.

## 0.11.17 (2026-04-23)

### Adaptive batch sizer in DiscStream — no more per-sector descent

Rip recovery rewritten. The old binary-search-per-bad-sector model paid the full descent (batch → half → quarter → … → single) for every bad sector in a region. On a damaged disc with 600 consecutive bad sectors this took 12+ hours. The new algorithm pays the descent once, remembers the working size, and ramps back up only after a sustained clean streak.

- **`BatchSizeChanged { new_size, reason }` event** — fires on shrink (read failed) and probe-up (clean streak threshold hit). Consumers use this to distinguish a "recovering" rip from a normal one.
- **Removed `BinarySearch` and `SectorRecovered` emissions from DiscStream** — no longer produced by the rip path. `SectorRecovered` still fires from `Drive::read`'s multi-phase recovery (unused by rips today, but kept for scan/other callers).
- **Removed `read_with_binary_search` and the 3×5s light-recovery loop** — no retry loops, no sleeps. One 5s attempt per read. On size-1 failure, skip (zero-fill) or error.
- **Probe-up threshold: 100 MiB (51,200 sectors) of clean reading at current size** before doubling toward preferred. Ramp 1 → preferred on good reading takes ~100 seconds for a typical BD — trivial vs. rip duration, conservative enough that a single lucky sector in a marginal zone can't trigger a premature probe.
- **Bad-region math**: ~600 consecutive bad sectors now complete in ~50 min (600 × 5s) instead of ~12h. The descent is O(log preferred) one time, not per sector.

### macOS

- Fix new clippy lint (`manual_c_str_literals`) in `scsi/macos.rs`.

## 0.11.16 (2026-04-21)

### API cleanup — one method per action
- **SectorReader::read_sectors(lba, count, buf, recovery)** — single method with `recovery: bool`. Removes `read_sectors_recover()`.
- **parser_for_codec(codec, codec_data)** — single constructor. Removes `parser_for_codec_with_data()`.
- **DvdSubParser::new(codec_data)** — single constructor. Removes `with_codec_data()`.
- **MkvMuxer::new(writer, tracks, title, duration, chapters)** — single constructor. Removes `new_with_chapters()`.

## 0.11.15 (2026-04-21)

### Lint cleanup
- Fix all `cargo fmt` and `cargo clippy -D warnings` across codebase.
- Remove unused imports, dead code, collapsible if-statements, div_ceil reimplementation.

## 0.11.14 (2026-04-21)

### Audit fixes: read recovery, verify, SCSI
- **Fix: trailing sectors at extent boundaries** — extents with sector_count not divisible by 3 no longer drop 1-2 trailing sectors. decrypt_sectors() safely skips partial AACS units.
- **Fix: verify_title stop support** — progress callback now returns bool. Return false to stop verification early instead of running to completion.
- **Fix: O_CLOEXEC on all SCSI fd opens** — prevents fd leak to child processes.
- **Fix: SCSI sense descriptor format** — correctly detect response code 0x72/0x73 (descriptor format) and extract sense key from byte 1 instead of byte 2.
- **Fix: DecryptFailed on missing unit key** — decrypt_sectors() returns Err(DecryptFailed) instead of silently using a zero key.

## 0.11.13 (2026-04-21)

### Fix: all rip reads use fast timeout
- Initial batch read changed from full Drive::read() recovery to fast 5s timeout. Binary search starts immediately on failure instead of after 10 minutes of retries.
- Max 15 seconds per bad sector (3 x 5s attempts). Max 23 seconds per batch with 1 bad sector.

## 0.11.12 (2026-04-21)

### Drive halt + sector events + light recovery
- **Drive.halt()** — AtomicBool flag checked between retry phases. Max 30s to stop.
- **Drive.on_event()** — callback for ReadError, Retry, SpeedChange, SectorRecovered events.
- **Error::Halted (E6010)** — distinct from DiscRead, indicates intentional stop.
- **Binary search light recovery** — single sectors get 3 attempts x 5s (15s max) instead of full 10-min Drive::read() recovery. Marginal disc zones complete in minutes not hours.
- **DiscStream.on_event()** — BinarySearch, SectorRecovered, SectorSkipped events.

## 0.11.11 (2026-04-20)

### Binary search error recovery
- **fill_extents binary search** — when a batch read fails, binary search to isolate the failing sector(s). Good sectors read in sub-batches at full speed. Only truly bad sectors get individual recovery. 60-sector batch with 1 bad sector: ~5 seconds instead of 10+ minutes.

## 0.11.10 (2026-04-20)

### Skip errors + clean verify API
- **DiscStream.skip_errors** — when true, zero-fills unreadable sectors and continues instead of aborting. Caller sets based on user preference.
- **read_sectors_recover(recovery: bool)** — single API for recovery vs fast reads. Replaces separate read_sectors_fast method.

## 0.11.9 (2026-04-20)

### Fast verify reads
- **read_sectors_fast()** — single-attempt 5s timeout SCSI read for verify. No recovery loop. Bad sectors detected in seconds instead of 10+ minutes.
- **SectorReader trait** — added read_sectors_fast() with default fallback to read_sectors().

## 0.11.8 (2026-04-20)

### Disc verify
- **verify::verify_title()** — sector-by-sector health check. Classifies sectors as Good/Slow/Recovered/Bad. Progress callback, chapter mapping, sector ranges.

## 0.11.7 (2026-04-19)

### TrueHD parser rewrite
- **12-bit length mask** — access unit length is lower 12 bits of first 2 bytes, not full 16. Upper 4 bits are parity nibble. Wrong mask caused misaligned frame splits.
- **AC-3 frame skipping** — BD-TS TrueHD PES contains interleaved AC-3 frames (same PID). Parser now detects AC-3 sync word (0x0B77) and skips those frames.
- **Cross-PES buffering** — access units that span PES packet boundaries are correctly reassembled.
- **Per-unit timestamps** — each access unit gets incrementing PTS (1/1200th second apart) instead of all units in one PES sharing the same timestamp.
- **Major sync detection** — keyframe flag set when access unit contains MLP major sync (0xF8726FBA).
- Result: zero TrueHD decode errors on UHD and BD (was ~19 per 30 seconds).

## 0.11.6 (2026-04-18)

### TrueHD fix (incomplete)
- Initial attempt at TrueHD header stripping — wrong approach, superseded by 0.11.7.

## 0.11.5 (2026-04-18)

### MKV container fixes — Jellyfin/player compatibility
- **Timestamp normalization** — MKV and M2TS output starts at 0.000s instead of raw disc PTS offset. Fixes playback failures in Jellyfin and other players.
- **DefaultDuration** — correct frame rate written to MKV track header. Fixes wrong avg_frame_rate (was 293/12, now 24000/1001).
- **HDR Colour metadata** — MatrixCoefficients, TransferCharacteristics, Primaries, Range written to MKV video track. Enables HDR tone mapping in players.
- **DisplayWidth/DisplayHeight** — aspect ratio fields in MKV video track.
- **Chapters (Blu-ray)** — accept mark_type 0 as chapter entry (was filtering to type 1 only, which no disc uses).
- **Chapters (DVD)** — extract chapter timestamps from PGC program map + cell durations.
- **Default disposition** — only first video and first audio track marked default. Fixes wrong auto-selection in players.

## 0.11.3 (2026-04-18)

### Unified versioning
- All freemkv repos now share the same version number. No functional changes from 0.10.10.

## 0.10.10 (2026-04-18)

### Dual-layer disc fix
- **UDF extent allocation** — use actual UDF allocation descriptors (`file_extents()`) instead of assuming m2ts files are contiguous from `file_start_lba`. Dual-layer UHD discs split large files across many extents (~1 GB each). The old single-extent assumption truncated rips at ~37% on affected discs.
- **Read error propagation** — `fill_extents()` returns `io::Result<bool>` so SCSI read errors propagate to the caller instead of being silently treated as EOF.

## 0.10.9 (2026-04-17)

### Fast disc identification
- **Disc::identify()** — reads UDF filesystem only (name, format, layers, encrypted). ~3s on USB vs 18s for full scan. No AACS handshake or playlist parsing.
- **KEYDB path fix** — added `~/.config/freemkv/keydb.cfg` to search paths. Fixes silent rip hang when KEYDB exists but isn't found by `resolve_keydb()`.

## 0.10.8 (2026-04-17)

### Buffered UDF reads
- **BufferedSectorReader** — prefetches batch sectors on single-sector reads. USB drives have ~500ms per SCSI command; this eliminates scan hangs.
- **Metadata partition pre-read** — loads entire UDF metadata partition into memory after initial parse.
- Scan time reduced from 10+ minutes to ~18 seconds on USB.

## 0.10.7 (2026-04-17)

### DiscStream::new()
- Replaced open_drive(), open_iso(), from_reader() with single new() constructor
- Stream accepts ContentFormat and sets up demuxer internally
- Removed disc:// case from input() — callers use primitives directly

## 0.10.6 (2026-04-16)

### Docker compatibility
- **Drive discovery** — removed sysfs check that blocked detection inside Docker containers. Device nodes are sufficient; INQUIRY command validates the device is an optical drive.

## 0.10.5 (2026-04-16)

### Audio parser buffering
- **AC3** — buffer across PES boundaries with frame size from fscod/frmsizecod table. Eliminates all AC3 decode errors on BD and UHD.
- **DTS** — buffer with core sync detection + frame size from header. DTS-HD extension frames handled correctly.
- **TrueHD** — buffer with unit length field parsing. Incomplete units held for next PES.
- All audio parsers now emit complete frames only. When PES boundaries align (normal case), buffering is a no-op.

## 0.10.4 (2026-04-16)

### CSS decryption — full key hierarchy
- **Bus auth → disc key → title key** — complete CSS key chain. Bus authentication with CSSCryptKey challenge-response, disc key decryption using 31 player keys via READ DVD STRUCTURE, title key extraction via REPORT KEY format 0x04.
- **CSS descramble cipher** — correct LFSR keystream generation with TAB5 for LFSR1 output and TAB4 for LFSR0 output. Per-sector key derivation from title key XOR sector seed.
- **Stevenson plaintext attack** — expanded pattern set (padding, video, audio, nav pack headers), scans up to 50K scrambled sectors for ISO key recovery.
- **Disc::copy() CSS decrypt** — sector-level decryption during disc→ISO copy produces clean ISOs with zero scramble flags.

### MPEG-2 PS demuxer fixes
- **DVD PS path routes through codec parsers** — was bypassing parser.parse(), producing raw PES frames without codec_private extraction or keyframe detection.
- **MPEG-2 sequence header extraction** — calculates exact header size including quantizer matrices (intra/non-intra flags), captures sequence extension from subsequent PES packets.
- **TsDemuxer dynamic PID table** — Vec instead of fixed [i16; 8192] for DVD PIDs that may exceed 8192.

## 0.10.3 (2026-04-16)

### DVD CSS authentication
- **CSS drive authentication** — full SCSI REPORT KEY / SEND KEY handshake with 6-round substitution-permutation cipher (CSSCryptKey). Brute-forces variant from 32 possibilities. Drive serves scrambled sectors after auth completes.
- **CSS auth runs before scan** — chicken-and-egg fix: auth must happen before reading VOB sectors for title key cracking, not after.
- **Remove debug output** — strip temporary eprintln from drive reads and CSS auth.

## 0.10.2 (2026-04-15)

### Fixes
- **Disc::copy() batch overflow** — hardcoded 64-sector batch exceeded BU40N's 60-sector hardware limit, causing every read to fail and trigger 5×30s recovery sleep. Now accepts detected batch size from caller, defaults to 60.
- **IFO PGC parsing** — playback time read from offset 0x04 (correct) instead of 0x02 (nr_programs). Cell BCD time at cell+4 not cell+0. DVD durations now correct.
- **Demuxer flush at EOF** — TS and PS demuxers flushed when source reaches EOF, preventing loss of last PES frame. Applied to DiscStream and M2tsStream.
- **DiscStream demuxer selection** — demuxer set by caller based on content_format (TS for Blu-ray, PS for DVD) instead of unconditionally creating TsDemuxer in from_reader()
- **StdioStream FMKV header** — writes/reads metadata header for roundtrip compatibility through stdio pipes

## 0.10.1 (2026-04-15)

### Architecture: streams are PES, disc.copy() for sector dumps
- **One stream per format, bidirectional PES** — MkvStream, M2tsStream, NetworkStream, StdioStream, NullStream each handle read and write
- **IsoStream merged into DiscStream** — one type for physical drives and ISO files, different SectorReader
- **Disc::copy()** — raw sector dump for disc→ISO, not a stream operation
- **IOStream deleted** — no more byte-level Read/Write on streams
- **ContentReader/OpenDisc deleted** — replaced by DiscStream + PES pipeline
- **CountingStream** — wrapper for progress tracking, no state in streams

### Error codes only — zero English in library
- All `io::Error::new(kind, "english")` replaced with `Error` enum variants
- New error variants: StreamReadOnly, StreamWriteOnly, StreamUrlInvalid, MkvInvalid, NoStreams, etc.
- `From<Error> for io::Error` — clean conversion at system boundaries
- Removed unused error variants: WriteError, ProfileNotFound, NotUnlocked, NotCalibrated, ScsiTimeout, etc.

### Deleted dead code
- `mkvout.rs`, `pesout.rs`, `isowriter.rs` — merged into parent stream types
- `lookahead.rs` usage in MkvStream — replaced by PES direct write
- ContentReader, OpenDisc, open_title() — replaced by PES pipeline
- `open_input()`, `open_output()` — replaced by `input()`, `output()`

## 0.10.0 (2026-04-15)

### PES pipeline
- **Unified Stream trait** — `read()` returns PES frames, `write()` accepts them. One trait for all streams.
- **All streams produce/consume PES frames** — DiscStream, IsoStream, MkvStream, M2tsStream, NetworkStream, StdioStream, NullStream
- **DVD PS demux** — MPEG-2 Program Stream demuxer produces PES frames
- **MKV input stream** — MKV demux produces PES frames
- **Network/stdio PES** — PES serialization over TCP and pipes
- **FileSectorReader** — ISO files implement SectorReader for unified disc/ISO handling

### PES pipeline audit (20 fixes)
- PES serialize: track/length validation, OOM cap (256 MB), stuffing compliance
- TsDemuxer: AF length validation, find_start_code verified
- PTS: marker bit validation, ns→90kHz saturating_mul, round-to-nearest
- AC3/DTS: debug_assert promoted to runtime check
- MKV: block_vint 3-4 byte support, track bounds check
- FMKV: JSON 10 MB cap, PAT section_len underflow guard

### codec_privates refactor
- **codec_privates on DiscTitle** — no separate parameter passing, no `_with_X` method variants
- **Streams-not-files** — MkvStream and M2tsStream take `impl Read`, not `File`/`Seek`
- **M2TS roundtrip fix** — TsMuxer Annex B conversion + codec_private in FMKV header
- **MKV remux fix** — MkvStream returns codec_privates from EBML header
- **Network codec_private fix** — FMKV header carries base64 codec_privates

### Cleanup
- Remove Seek/File dependencies from stream interfaces
- Remove eprintln from library code
- Fix all clippy warnings
- 342 tests pass

## 0.9.0 (2026-04-14)

### Drive recovery + decrypt architecture
- **Drive::read()** — single read method with built-in error recovery (min speed → reset → retry)
- **Decrypt in streams** — streams handle their own decryption via `decrypt_sectors()`. Pipeline just moves bytes.
- **keys() on IOStream** — streams report their own decrypt keys
- **InputOptions** — `--raw` wired through to streams, skips decrypt only
- **decrypt_sectors returns Result** — fail instead of silent corruption
- **Handshake fix** — no longer returns fake success on failure
- **Drive::read_capacity()** — for raw sector dump (disc→ISO)
- **Reset on open** — SgIoTransport resets device on every open
- **Simplified DiscStream** — removed on_error/on_success/Recovery enum

### Platform
- **Rust 1.86 MSRV** pinned in Cargo.toml and CI
- **macOS build fix** — MacScsiTransport marked Send
- **is_multiple_of** — replaced nightly API with stable equivalent

### API changes
- **Drive object** — typed DriveSession API
- **Typed StreamUrl** — URL parsing returns enum, not strings
- **DriveStatus API** — reset(), wait_ready with fallback
- **Granular SCSI queries** — individual methods on DriveSession for capture
- **Profile module public** — for external tools (bdemu)
- **Tray lock/unlock** — exposed on Drive

## 0.8.0 (2026-04-11)

### DVD support
- **Full DVD pipeline** — VIDEO_TS detection, IFO parsing, CSS decryption, MPEG-2 PS demuxing
- **CSS cipher** — Stevenson 1999 table-driven implementation, no keys needed
- **IFO parser** — title sets, PGC chains, cell addresses, audio/subtitle attributes, palette
- **MPEG-2 PS demuxer** — pack headers, PES extraction, private stream 1 sub-streams
- **MPEG-2 video parser** — sequence headers, I-frame detection, codec_private

### 100% codec coverage
- **E-AC-3 (Dolby Digital Plus)** — bsid detection, frame size calculation
- **DTS-HD MA/HR** — extension substream detection and inclusion
- **LPCM** — BD header skip, raw PCM extraction
- **DVD subtitles (VobSub)** — passthrough with IFO palette extraction (YCbCr→RGB)
- **Dolby Vision** — verified RPU NAL type 62 preserved in HEVC passthrough

### MKV improvements
- **Chapters** — MPLS PlayList marks → MKV Chapters element
- **Track flags** — FlagDefault, FlagForced, Language correctly set
- **HEVC codec_private** — profile compatibility and constraint flags from SPS
- **VC-1 codec_private** — resolution parsed from sequence header

### Architecture
- **SectorReader trait** — decouples disc scanning from SCSI
- **Disc::scan_image()** — scan ISO images or any SectorReader
- **resolve_encryption()** — single function handles AACS 1.0/2.0/CSS/none
- **Module refactors** — disc/ (4 files), aacs/ (5 files), drive/ (3 files)
- **Module visibility** — internal modules pub(crate), explicit AACS re-exports

### Streams
- **StdioStream** — stdin/stdout pipe
- **IsoStream** — read/write Blu-ray ISO images with UDF 2.50 filesystem
- **Strict URLs** — all URLs require scheme:// prefix, bare paths rejected
- **total_bytes()** — IOStream reports content size for progress display

### Platform
- **Windows SPTI** — SCSI Pass-Through Interface backend
- **Windows builds** — CI + release workflow for x86_64-pc-windows-msvc
- **macOS drive discovery** — separate from Linux (drive/macos.rs)
- **Stable download URLs** — /latest/download/ with version-free filenames

### Audit fixes (4 rounds, 14→0 critical)
- UDF bounds checking on all disc-sourced offsets
- SCSI: Linux residual underflow, macOS task_status type, Windows buffer zeroing
- AACS: EC mod_inv safe, key reduced mod n, host cert fallback
- DiscStream: persistent read state (was recreating ContentReader per call)
- ISO writer: UDF tag checksums, multi-extent >4GB, reserve AVDP placement
- CSS crack: labeled loop break, polynomial match
- 0 clippy warnings

### Testing
- **327 tests** (was 64 at start)
- CSS/AACS cross-validation against independent AES implementation
- End-to-end MKV mux test with H.264 codec headers

## 0.7.2 (2026-04-11)

### Windows support

- **SPTI backend** (`scsi/windows.rs`) — SCSI_PASS_THROUGH_DIRECT via DeviceIoControl
- **Windows drive discovery** (`drive/windows.rs`) — scans CdRom0-15 + drive letters
- **Platform file separation** — `drive/unix.rs` and `drive/windows.rs`, no inline cfg branches
- **CI** — `cargo check` on windows-latest, actions/checkout@v5

### Test suite

- **177 tests** (was 64) — MPLS, CLPI, H.264, HEVC, AC3, VC1, DTS, TrueHd, PGS, EBML, UDF, disc scanning, streams
- **FEATURES.md** created

### Improvements

- **Stable download URLs** — `/latest/download/freemkv-x86_64-unknown-linux-musl.tar.gz` works forever

## 0.7.1 (2026-04-11)

### SectorReader trait

- **`SectorReader` trait** — decouples disc scanning from SCSI. UDF, MPLS, CLPI, labels, and AACS resolution now work with any sector source.
- **`Disc::scan_image()`** — scan ISO images or any SectorReader. Full title/stream/label/AACS pipeline, no drive required.
- **`resolve_encryption()`** — single function handles AACS 1.0, 2.0, or none. Uses whatever path works (KEYDB VUK, handshake, media key, device key).

### Stream types

- **7 stream types** — Disc, ISO, MKV, M2TS, Network, Stdio, Null
- **`IsoStream`** — read/write Blu-ray ISO images. Uses `Disc::scan_image()` for full UDF parsing (not heuristic scanning).
- **`StdioStream`** — stdin/stdout pipe, format-agnostic
- **Strict URL format** — all URLs require `scheme://path`. Bare paths rejected with clear error messages.
- **Validation** — empty paths, missing ports, read-only/write-only direction errors

### IOStream trait

- `IOStream` trait for all stream types (Read + Write + info + finish)
- `open_input()` / `open_output()` resolve URL strings to stream instances

## 0.7.0 (2026-04-11)

### Stream I/O architecture

- **5 stream types** — Disc, MKV, M2TS, Network, Null
- **`IOStream` trait** — common interface for all streams
- **URL resolver** — `open_input()` / `open_output()` with scheme://path format
- **FMKV metadata header** — JSON metadata embedded in M2TS and network streams
- **Bidirectional MKV** — MkvStream reads and writes Matroska containers
- **Network streaming** — TCP with metadata header, TCP_NODELAY
- **BD-TS demuxer** — PAT/PMT scanning, PTS duration detection
- **EBML reader** — parse existing MKV files for read-side MkvStream

## 0.6.0 (2026-04-10)

### API improvements

- **`open()` works on all drives** — no profile match required. Unknown drives can scan, read BD/DVD at OEM speed. `init()` is optional and adds features (riplock removal, UHD reads, speed control).
- **`has_profile()`** — check if unlock parameters are available for this drive
- **`find_drives()`** — returns all optical drives, not just profile-matched ones
- **`raw_gc_010c`** on `DriveId` — raw GET_CONFIG 010C response bytes for profile sharing

### AACS 2.0

- **SCSI handshake wired end-to-end** — ECDH key agreement, real Volume ID from drive, read data key for bus decryption
- **Bus decryption active** — UHD discs with bus encryption now decrypted transparently
- **VUK derivation from Media Key + VID** — works for discs not in KEYDB (processing key + device key paths)

### MKV muxer

- **15 new files** — EBML writer, TS demuxer, stream assembly pipeline
- **Codec parsers** — H.264, HEVC, AC-3, DTS, TrueHD, PGS, VC-1
- **`MkvStream`** — builder pattern, wraps any `impl Write`, configurable lookahead buffer

### Cleanup

- Removed orphaned `jar.rs` (342 lines) — replaced by `labels/` module
- Error refactor: 40+ sites converted from English strings to typed error codes

## 0.5.0 (2026-04-09)

### Read pipeline — 5x speed improvement

- **Kernel transfer limit detection**: auto-detect `max_hw_sectors_kb` via sysfs, resolve sg→block device. Previously hardcoded to 510 sectors (1MB) which exceeded the 120KB kernel limit, causing all reads to error and fall back to 6KB reads at 4.8 MB/s. Now auto-tunes to 48 sectors (96KB) or whatever the device supports.
- **Result: 12.5 MB/s sustained, 23 MB/s peak** (was 4.8 MB/s)

### LibreDrive — full init pipeline

- **All 10 ARM handlers translated**: unlock, firmware upload (A: WRITE_BUFFER, B: MODE SELECT), calibrate (256 zones), register reads, status, probe, set_read_speed, keepalive, timing
- **Cold boot firmware upload**: WRITE_BUFFER 1888B (A variant) or MODE SELECT 2496B (B variant) proven on hardware
- **Speed calibration**: 256+ disc surface probes, 64-entry speed table, triple SET_CD_SPEED
- **Platform trait locked down**: `pub(crate)`, 3 methods only (init, set_read_speed, is_ready)
- **Init guard**: prevents double-init, signature mismatch aborts early

### MPLS parser fixes

- **PGS in audio slots**: subtitle language read at correct offset (was truncated: "ng " → "eng")
- **Secondary PG entries**: n_pip_pg loop added for correct STN position tracking
- **Secondary stream types**: stream_type 5 (sec audio), 6 (sec video), 7 (DV EL) attribute parsing
- **Empty stream filter**: coding_type 0x00 entries (padding) no longer appear as "Unknown(0)"

### Profiles

- **206 profiles with full per-drive data**: ld_microcode (base64), all CDBs, speed tables, signatures
- **Automated pipeline**: `sdf_unpack --profiles` → profiles.json (no manual merging)

## 0.4.0 (2026-04-07)

### Labels — complete rewrite

- **Detect-then-parse architecture**: each BD-J authoring format has its own parser module with `detect()` and `parse()` functions. Drop in a new parser with one line in the registry.
- **5 format parsers**: Paramount (`playlists.xml`), Criterion (`streamproperties.xml`), Pixelogic (`bluray_project.bin`), Warner CTRM (`menu_base.prop` / `language_streams.txt`), shared label vocabulary (`vocab.rs`)
- **Raw disc data principle**: label data passes through as-is from disc. Only BD-standard codec identifiers (MLP, AC3, DTS) are mapped to display names. Unknown authoring tool codes (csp, eda, cf) pass through raw.
- **`variant` field**: replaces `region` — language dialect codes from authoring tools, not BD spec regions
- Removed: old `jar` module (superseded by labels)
- Removed: dead label apply functions from disc.rs

### Drive

- **`DriveSession::eject()`**: sends PREVENT ALLOW MEDIUM REMOVAL then START STOP UNIT. Works reliably after raw mode unlock.
- **`DiscRegion` enum**: Free, BluRay(A/B/C), Dvd(1-8). UHD always region-free.

### Capture

- **Fixed sector range collection**: captures ALL files on disc (only skips STREAM/ video files and >50MB). Previously skipped BACKUP/, DUPLICATE/, and files >10MB which missed JAR content.

## 0.3.1

- Labels module: 4 disc file parsers for stream labels
- Simplified labels API

## 0.3.0

- Initial public release
- SCSI transport (Linux SG_IO, macOS IOKit)
- UDF 2.50 filesystem reader
- MPLS/CLPI parsers with full STN support
- Drive identification + profile matching
- 206 bundled drive profiles
- AACS 1.0 decryption (VUK + unit keys)
