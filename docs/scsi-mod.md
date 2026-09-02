# src/scsi/mod.rs — relocated internal notes

Overflow detail for internal (non-public) comments in `src/scsi/mod.rs`,
moved here to keep the guard's internal-comment cap. Each section is
pointed to by a short `// See docs/scsi-mod.md — ...` comment at its
original site.

## READ_TIMEOUT_MS calibration

10 s is calibrated from live empirical data on an LG BU40N + Initio 1618L
bridge ripping a UHD with marginal sectors:

- Sustained sequential reads: 3–7 ms
- Cold-start seek + read: up to ~1500 ms
- Successful ECC recovery: 1.6–2.6 sec
- Confirmed unreadable sector: 3.6–8.8 sec (kernel timeout)

10 s catches every legitimate slow read with comfortable margin and
short-circuits truly bad sectors at ~10 s rather than letting the kernel
mid-layer escalate for 30 s+.

Pre-0.13.21 this was 1.5 s, which forced the kernel mid-layer to time out
*normal* reads (cold-start often takes ~1.5 s) and run its full ABORT
TASK / LUN RESET / BUS RESET escalation while userspace kept submitting
fresh reads. The Initio bridge couldn't drain the resulting command queue
and entered a wedge state that only physical replug recovered — proven by
the v0.13.18 + v0.13.20 live tests.

Consumed only by the ripping read path (`drive::read`); gated so a
transport-only build doesn't warn on an unused const.

## READ_RECOVERY_TIMEOUT_MS history

Matches `sg_dd`'s 60 s ceiling: long enough that any sector the drive can
recover at all gets the time to do so, short enough that an unresponsive
bus is detected before the per-range watchdog fires.

In practice failed reads return in 1–4 s (the drive itself gives up on
uncorrectable ECC before the timeout); the 60 s value is a safety
ceiling, not a steady-state cost.

Historical note (2026-05-08): briefly lowered to 2 s with a 5x inline
retry loop in `freemkv_engine::recovery::patch` to mimic the kernel
`sr_mod` driver's auto-retry pattern. The synthetic logic worked but on
the live drive each "2 s" read paid ~1.5 s of kernel SCSI mid-layer
error escalation on top, so 5x retries took ~17 s per LBA and triggered
MAX_RANGE_SECS after 4 sectors — pushing recovery to 0/22 ranges (worse
than the 0/22 baseline of v0.17.3 single-shot at 60 s, since that at
least visited every range). Reverted; the kernel-auto-retry approach is
being pursued via a `/dev/sr0` pread fallback instead.

## checked_cdb_len rationale

Under SPC-4 the opcode's group code (bits 7-5 of byte 0) fixes the CDB
length, so dropping the tail bytes does not produce a shorter form of the
same command — it produces a DIFFERENT command with a different meaning
for the bytes that remain. The drive will usually execute it and return
GOOD status with data for a request the caller never made: a silently
wrong result, on the transport layer everything else in the crate sits
on.

Lives here, shared by all three platform backends, so the guard cannot
drift per platform (it previously truncated on Linux and Windows while
erroring on macOS — the "works on my platform, not theirs" class).

An EMPTY CDB is rejected here too. `ScsiTransport` is a public trait, so
an out-of-crate caller can pass one; every backend then either indexes
`cdb[0]` (a panic out of a public API) or hands the driver a zero-length
command descriptor, which under SPC-4 is not a command at all. That
guard used to exist ONLY in the Linux backend — macOS and Windows had
nothing — which is the same per-platform drift this helper exists to
prevent.

## parse_sense format details

Handles both response-code formats SPC-4 mandates:

- **Descriptor format** (response code `0x72` / `0x73`):
  - sense key = `sense[1] & 0x0F`
  - asc = `sense[2]`
  - ascq = `sense[3]`
- **Fixed format** (response code `0x70` / `0x71` and any unknown code
  per SPC-4 §4.5.3):
  - sense key = `sense[2] & 0x0F`
  - asc = `sense[12]`
  - ascq = `sense[13]`

`sb_len_wr` is the number of bytes the transport actually wrote into
`sense`. When the buffer is too short for the relevant fields we return
[`ScsiSense::NONE`] for the missing pieces rather than reading
uninitialised memory. The minimum useful sense reply is 4 bytes
(descriptor, to reach ASCQ at offset 3) or 14 bytes (fixed, to reach
ASC/ASCQ at offsets 12/13).

Pure function — same parse on every platform backend (Linux SG_IO, macOS
IOKit, Windows SPTI) so a regression here would silently mis-route SCSI
errors on all three OSes simultaneously.

## is_marginal detail

- `MEDIUM ERROR` (3) — canonical bad-sector signal
- `NOT READY` (2) — on many drives (notably BU40N), this is the dominant
  response for unreadable sectors (ASC 04/3E, 04/01, etc.)
- `ABORTED COMMAND` (B) — transient; retry usually works
- `RECOVERED ERROR` (1) / `NO SENSE` (0) — drive is healthy and either
  recovered the data or has no specific fault to report

## max_transfer_bytes Windows rationale

The Windows backend overrides the 1 MiB default with the adapter's real
`MaximumTransferLength` (queried via `IOCTL_STORAGE_QUERY_PROPERTY`): a
16 MiB READ that exceeds the adapter limit makes `DeviceIoControl` fail
outright, which freemkv then mis-reads as a transport failure and falls
back to slow, log-spamming tiny reads. Chunking to this limit fixes
that. Linux/macOS keep the 1 MiB default (well within any real
`max_sectors_kb`).

## drive_has_disc no-recovery rationale

A single TUR is issued; nothing else. No SCSI bus reset, no USB device
reset, no retry is attempted in-library (the USB-reset escalation was
removed in 0.13.4 after it was shown to deepen rather than clear the
wedge). No SCSI primitive is exposed to outside crates — autorip /
freemkv CLI / bdemu use this single function for the entire "is there a
disc?" decision.

## scsi module reset history

A top-level `scsi::reset()` wrapping platform reset in a
thread+recv_timeout used to live here; removed in 0.13.6 along with the
SG_SCSI_RESET / STOP+START UNIT escalation it existed to guard.

USB-layer recovery (`scsi::usb_reset()`) was rolled back in 0.13.4: on
the LG BU40N the USB reset itself succeeds but firmware below the bridge
stays locked until unplug-replug. See git tag `v0.13.3`.

The only hardware-touching APIs autorip + freemkv CLI use outside the
rip path: a one-shot `list_drives()` enumeration and a single-TUR
`drive_has_disc(path)`. `Drive::open`/`init()`/`Disc::scan` stay heavy.

## align_up mask semantics

`mask` is a *mask*, not a power-of-two alignment value: `0` means "no
alignment requirement" (any address is fine), `1` means 2-byte, `3` means
DWORD (4-byte), `7` means 8-byte, etc. — always one less than the
required alignment. An address is acceptable iff `(addr & mask) == 0`.

Returns the smallest `addr >= p` with `(addr & mask) == 0`. The standard
branch-free idiom `(p + mask) & !mask` works for any valid (`2^n - 1`)
mask, including `mask == 0` (where it is the identity).

Lives here, compiled on every platform, so the Windows SPTI bounce
buffer in `windows.rs` can share it and so the arithmetic gets unit
coverage on macOS/Linux CI even though the SPTI path only builds on
Windows.
