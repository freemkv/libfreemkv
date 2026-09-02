# identity.rs — drive identity test notes

## `OversizedCountTransport` (test double)

Returns the requested data length but reports a `bytes_transferred` larger
than the caller's buffer — models a drive that lies about its transfer
count. The old slicing code panicked on this; the clamps in `from_drive`
must keep it from indexing out of range.

## `inquiry_with_a_short_data_phase_fails_instead_of_reporting_a_blank_drive`

`ascii_field` with a buffer shorter than `start` returns empty string
rather than panicking.

- Spec: SPC-4 §6.4.2 — bytes[8:16] are vendor ID; a truncated buffer (e.g.
  a device that reports fewer than 8 bytes) must not panic.
- Mutation: removing the `data.len() > start` guard makes it panic on
  short inputs.

A drive that answers INQUIRY with GOOD status but a short or empty data
phase must fail the probe, not present as a blank drive.

The buffer is pre-zeroed, so decoding it unconditionally yielded empty
vendor/product/revision strings and a byte 0 of 0x00. Every platform
enumerator gates on `raw_inquiry[0] & 0x1F == SCSI_PERIPHERAL_TYPE_OPTICAL`,
and 0x00 is DIRECT ACCESS — so the drive silently vanished from the device
list rather than reporting that its identity probe failed. A USB-SATA
bridge mid-wedge does exactly this.

The two GET CONFIGURATION calls in the same function already clamped on
`bytes_transferred`, with a comment calling it untrusted; INQUIRY, three
lines above them, discarded it.

## `ascii_field_boundary_len_equals_start_is_empty`

`ascii_field`'s guard is `data.len() > start` (strictly greater), not
`>=`: a buffer whose length is exactly `start` has NO byte at that offset,
so it must still yield empty, not attempt to slice.

Mutation: `>` -> `>=` would try to slice `data[start..]` when
`data.len() == start`, which panics (empty range at the very end is fine,
but the guard's job is the `< start` case below it — pinning the exact
boundary catches an off-by-one either direction).

## `display_formats_trimmed_fields_space_separated`

`Display` renders the four trimmed identity fields space-separated — the
human-readable counterpart of `match_key`'s pipe-separated form. Not
exercised anywhere else in this test module.

Mutation: replacing the `fmt` body with `Ok(Default::default())` writes
nothing at all, so formatting any `DriveId` yields "".

## `FixedGcCountTransport` (test double) / `from_drive_gc_failure_yields_empty_firmware_date`

GET CONFIGURATION failure (transport error) must not abort the identity
probe — firmware_date is empty, raw_gc_010c is empty. Mutation:
propagating the GET_CONFIGURATION error with `?` aborts `from_drive`.

`FixedGcCountTransport` is a transport whose GET CONFIGURATION responses
report an exact, caller-chosen `bytes_transferred` for each of the two GC
features (010Ch firmware date / 0108h serial), so the `end > 12` / `> 12`
boundary guards can be pinned precisely. INQUIRY always succeeds.

## `from_drive_firmware_date_boundary_exactly_12_is_empty`

`end > 12` in the firmware-date branch (`from_drive`) is a strict
inequality: `bytes_transferred == 12` reports the field absent (offset 12
is the first byte of the 12-char date; a count of exactly 12 covers bytes
0..12, none of which is the date), so `firmware_date` must be empty, not
the mutant's off-by-one read.

Mutation: `>` -> `>=` would try `gc[12..12]` at the boundary — an empty
but non-panicking slice — silently reporting "present" data that is
actually all outside the transferred count.
