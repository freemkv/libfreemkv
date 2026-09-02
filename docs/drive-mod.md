# src/drive/mod.rs — internal notes

Long-form rationale relocated from `src/drive/mod.rs` comments that exceeded
the comment-guard's internal-comment cap. Each section is pointed to by a
short `// See docs/drive-mod.md — <topic>` comment at its original site.

## Error-recovery mode page (PER bit)

`MODE_PAGE_ERROR_RECOVERY` (0x01) is the SBC/MMC Read-Write Error Recovery
mode page. We flip the `PER` bit to make the drive REPORT a recovered read
(via CHECK CONDITION + sense key RECOVERED ERROR) instead of silently
returning best-effort data as GOOD status. On marginal/dirty media that
silent-GOOD data can be mis-corrected — a rip that "passed clean" but decoded
with errors. With PER on, freemkv sees the marginal read and re-reads it in
Pass N (a loud miss, never a silent commit). See
`build_error_recovery_select_payload`.

## `sleep_until_halted` — why it exists and why it was dormant

This was `#[cfg(test)]` for two releases, kept alive only by its unit tests,
while the two production paths that actually sleep — the `wait_ready` poll
backoff (60 x 500 ms) and `spin_cycle`'s spin-down/settle pauses
(`SPIN_DOWN_IDLE_SECS` + `SPIN_UP_SETTLE_SECS`) — used a plain
`std::thread::sleep` and so were deaf to the operator's Stop for ~30 s and
~15 s respectively. Every other drive path returns `Halted` at the next
`checked_exec` boundary; a Stop pressed during spin-up simply did nothing
visible until the poll ran out. The primitive existed; the sleeping code just
did not call it. It is production code again.

## `Drive::read` — single-shot contract and the removed inline retries

`recovery=true` uses `crate::scsi::READ_RECOVERY_TIMEOUT_MS` (60 s, matches
sg_dd) for the `freemkv_engine::recovery::patch` pass; `recovery=false` uses
`crate::scsi::READ_TIMEOUT_MS` (10 s) for `freemkv_engine::recovery::copy`'s
fast skip-forward sweep. Both budgets are generous enough that the drive can
finish ECC recovery on a marginal sector — pre-0.13.21 this was 1.5 s on the
fast path which forced the kernel mid-layer to time out and escalate while we
waited anyway. On any failure this returns `Err(DiscRead)` immediately;
orchestration (`freemkv_engine::recovery::patch` multi-pass, `DiscStream`
adaptive batch halving) handles retry policy.

Inline retry phases (5x gentle + reset+reopen + 5x more) were removed in
0.13.6: on some USB-SATA bridges the inline reset wedged drive firmware
without ever recovering a sector. The remaining recovery layers
(`freemkv_engine::recovery::patch` multi-pass, `DiscStream` batch halving) do
not touch the wedge-prone reset path.

## `find_drive` — media-preference selection policy

On a multi-drive system (common on Windows, where an empty/not-ready drive
can enumerate first) returning the first drive blindly can pick a drive with
no disc, dooming the operation. So `find_drive` opens each candidate in
enumeration order, queries `Drive::drive_status` (GET EVENT STATUS, which
works regardless of firmware state), and returns the first drive reporting
`DriveStatus::DiscPresent`. If no drive reports a disc — or `drive_status()`
is unavailable/returns `Unknown` everywhere (single-drive or quirky bridges)
— it falls back to the first drive that opened, preserving historical
behavior so those setups don't regress. For just listing drives without
opening (e.g. UI sidebar), use `scsi::list_drives()` instead — it returns
`DriveInfo` (path + identity) without the cost of running every drive's
profile + identity probe.

## `open_block_device_for_sg` — why no `O_DIRECT`

Resolves a `/dev/sg*` path to the corresponding `/dev/sr*` block device by
walking sysfs, then opens it for read. No `O_DIRECT`:
`posix_fadvise(POSIX_FADV_DONTNEED)` flushes the cache before each pread,
which avoids buffer-alignment requirements while still forcing fresh device
reads. Returns `None` on any error (sysfs not present, no matching block
device, open failed); callers treat that as "no fallback available" and
propagate the original SCSI READ error.

## `resolve_device` — staged, not yet wired

The cross-platform dispatch is kept ready for the caller that will consume
it, so the per-platform implementations below it (and their tests) stay
live. `allow(dead_code)` marks that deliberately — this is not an accidental
orphan.

## Test rationale: recovery/halt/chunking coverage

The `halt_tests` and `command_tests` modules exercise mutation-sensitive
edges (halt-aware sleep timing, chunked-read offset arithmetic, drive-status
byte decoding, spin-cycle/wait-ready halt responsiveness). The following
notes preserve the "why this exact scenario" reasoning that the inline test
doc comments were trimmed from:

- **`disc_is_dvd_matches_only_dvd_profile_family`**: must match the DVD
  profile family (0x0010..=0x001F) and ONLY that family. A false positive on
  a BD/UHD profile (0x0040+) would skip the drive unlock UHD reads require; a
  false negative on a DVD would re-introduce the CSS read failure. Widening
  the range to `..=0x0040` makes the BD-ROM assert fire; a failed/short GET
  CONFIGURATION must default to NOT-DVD so the unlock still runs.

- **`media_event_reply` / media-status decoding**: MMC-6 §6.7 — byte 5 of the
  GET EVENT STATUS reply is a Media Status only when the Event Header says a
  media event descriptor follows (NEA clear AND Notification Class == 4).
  Decoding byte 5 regardless of those preconditions reads a reserved/zero
  byte as Media Status 0 and reports NoDisc on a drive that has a disc loaded
  — the classic "works on my drive, not theirs" firmware split. The drive is
  untrusted input, so an event-less reply must fall back to the TEST UNIT
  READY status instead.

- **Chunked-read tests** (`read_chunks_write_into_correctly_offset_buffer_regions`,
  `an_undersized_buffer_errors_on_the_chunked_path_just_like_the_single_one`,
  `chunk_lba_near_u32_max_errors_not_overflows`): the multi-chunk path slices
  the caller's buffer by `count * 2048` and advances the per-chunk LBA with
  `lba + done`, both unchecked. An undersized buffer PANICKED
  ("range end index out of range") out of the public `read`/`read_fua`
  instead of returning `Err(DiscRead)` like the single-chunk path; an LBA
  range crossing `u32::MAX` overflowed (debug panic, or release wrap to a low
  LBA silently read). These tests pin both to error-not-panic/overflow, and
  one writes a distinct marker per chunk to confirm the byte-offset
  arithmetic (`done * 2048`, not `done + 2048`) is correct — no other mock
  touches the buffer contents, so a `*` -> `+`/`/` mutation would otherwise go
  unasserted.

- **`wait_ready`/`spin_cycle` halt tests**: `wait_ready` was the one drive
  path that called `self.scsi.as_mut().execute(..)` instead of
  `checked_exec`, and its 60 x 500 ms loop never read `self.halt` — a cancel
  during spin-up was ignored for ~30 s. `spin_cycle` had the same gap across
  two bare `execute` calls and two plain `std::thread::sleep` pauses
  (`SPIN_DOWN_IDLE_SECS` + `SPIN_UP_SETTLE_SECS`), and it runs from the
  recovery path — precisely when an operator is most likely to press Stop.
  The tests pin: a Stop set before the call returns `Halted` immediately with
  no command issued; a Stop set mid-poll (on a deterministic Nth call, no
  wall-clock race) exits at the next `checked_exec` boundary; a Stop that
  lands during the spin-down pause wakes the halt-aware sleep within
  ~100 ms instead of blocking for the full 5 s.

- **`read_logs_a_good_status_short_transfer`**: a READ(10) that completes
  with GOOD status but a residual underrun is correctly refused, but the
  refusal used to log nothing — so a drive that residual-underruns on GOOD
  status produced the same silent journal as a scratched disc, and the
  operator was sent after the wrong remedy (replace the drive vs. clean the
  disc). The test pins that the refusal now warns with lba/transferred/
  expected/code.

## `open_block_device_for_sg` doc contract (kept short in code)

See the section above for the `O_DIRECT` rationale; the pointer comment near
the function keeps only the API contract.
