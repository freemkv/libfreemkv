# Linux SCSI transport (`src/scsi/linux.rs`)

## Kernel error-handling ladder

No userspace abort, no fd close+reopen, no SG_SCSI_RESET escalation — the
kernel SCSI mid-layer's `scsi_eh.rst` ladder (ABORT TASK -> LUN RESET ->
BUS RESET -> HOST RESET) runs internally when `hdr.timeout` expires, and
by the time the ioctl returns the kernel has already done what it can.

## Why one synchronous ioctl

This matches established practice for optical/SCSI I/O: a single
synchronous ioctl with a bounded per-command timeout (commonly in the
8-60 s range), consistent with the Linux kernel default for SCSI block
devices (30 s `/sys/.../timeout`).

## History: the async design it replaced

Pre-0.13.20 we ran an async `write() + poll(1.5s) + close-on-timeout +
bg reopen` pattern. That abandoned slow-but-alive commands faster than
the drive could drain its internal queue, deepening the wedge
pattern on the LG BU40N. Reverted in 0.13.20.

## `execute()` details

One syscall: `ioctl(fd, SG_IO, &hdr)`. The kernel honors `hdr.timeout`
and runs its own ABORT TASK -> LUN RESET -> BUS RESET -> HOST RESET
escalation if the device times out (per `Documentation/scsi/scsi_eh.rst`).
By the time this returns, the kernel has done its recovery work.

Errors surfaced to the caller:

- ioctl returned -1 -> `Error::IoError` (kernel-level failure)
- `hdr.host_status` != 0 OR `(hdr.driver_status & ~DRIVER_SENSE)` != 0
  -> `Error::ScsiError { status: 0xFF, sense_key: 0, asc: 0, ascq: 0 }`
  (real transport-layer failure: kernel timeout, bridge wedge, bus error)
- `hdr.status` != 0 (typically `0x02` CHECK CONDITION) ->
  `Error::ScsiError { status, sense_key, asc, ascq }` carrying the
  drive's full SPC-4 sense triple. Callers route on
  `is_medium_error()`, `is_unit_attention()`, etc.

SG's `DRIVER_SENSE` (0x08) bit indicates *sense data is attached* — it's
set on every CHECK CONDITION reply. It is **not** a transport failure;
pre-0.13.23 we conflated it with one and silently lost every
drive-reported error reason. The mask in the transport-error check is
the fix.

Caller's `data` buffer is mutated only on success; partial transfers
are reported via `bytes_transferred = data.len() - resid`.
