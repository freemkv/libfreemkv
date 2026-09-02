# SCSI error-decoding test contract

Background for `tests/scsi_error_decoding.rs`.

v0.13.20 rewrote `scsi/linux.rs` to a synchronous blocking SG_IO and
consolidated sense parsing into the `parse_sense` helper that every
platform backend now shares. v0.13.23 replaced the `Error::ScsiError`
flat-fields shape with `{ opcode, status, sense: Option<ScsiSense> }`
so callers can route on structured sense data (key + ASC + ASCQ) via
`Error::scsi_sense` / `Error::is_marginal_read` / `ScsiSense::is_*`.

The actual `ioctl(SG_IO, ...)` call is impossible to mock without a
kernel, so libc shims are deliberately avoided here. These tests
therefore pin the *contract* every backend must satisfy via a mock
`ScsiTransport`:

1. Healthy result -> `Ok(ScsiResult { bytes_transferred = data.len() - resid })`.
2. Transport-level failure (no SCSI status delivered: kernel
   timeout, USB bridge wedge, IOKit service error) ->
   `Error::ScsiError { status: SCSI_STATUS_TRANSPORT_FAILURE, sense: None }`.
   `Error::is_scsi_transport_failure()` returns `true`. Used by
   `drive_has_disc` to detect the wedge signature.
3. SCSI-level failure (drive replied CHECK CONDITION with sense) ->
   `Error::ScsiError { status: 0x02, sense: Some(ScsiSense {...}) }`
   with the parsed key/ASC/ASCQ.
4. `Error::is_marginal_read()` is `true` for MEDIUM ERROR /
   ABORTED COMMAND / RECOVERED ERROR / NO SENSE; `false` for
   HARDWARE / DATA PROTECT / UNIT ATTENTION / NOT READY / ILLEGAL
   REQUEST and for transport failures.

Inline `parse_sense_tests` in `src/scsi/mod.rs` cover the pure parse
logic (descriptor 0x72/0x73 vs fixed 0x70/0x71, short-buffer, VALID
bit masking, unknown response codes, ASC/ASCQ offsets); this file
covers the consumer side -- a real transport feeding a real Error
variant to a real call site (`scsi::inquiry`).

## MockTransport outcomes

Outcomes mirror what each backend's `execute()` should produce after
the v0.13.23 sense plumbing: `Option<ScsiSense>` carrying the full
SPC-4 triple for drive-reported failures, `None` for transport-level
failures.
