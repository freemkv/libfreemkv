# macOS SCSI transport (`src/scsi/macos.rs`)

## Open sequence (C shim, `macos_shim.c`)

`MacScsiTransport::open` drives all CDBs (INQUIRY, READ, REPORT KEY, etc.)
through `SCSITaskDeviceInterface::ExecuteTaskSync` — 1:1 with the Linux
SG_IO backend. Getting there involves the shim doing:

1. `diskutil unmountDisk force` on the target device only
2. Find `IOBDServices` matching the requested BSD name (walks IOKit
   registry: IOBDServices -> IOBDBlockStorageDriver -> IOMedia -> BSD Name)
3. Create `MMCDeviceInterface` -> `SCSITaskDeviceInterface`
4. `ObtainExclusiveAccess`
5. Raw CDB dispatch via `CreateSCSITask` + `ExecuteTaskSync`

## Single-instance ownership (`OPEN` flag)

The C shim uses one global IOKit handle (`g_handle`), so only one
`MacScsiTransport` may exist at a time: a second `open()` would share
that handle, and the first `drop()` would tear it down out from under
the other. The `OPEN` `AtomicBool` enforces single-instance ownership,
independent of the shim's own state.

## `map_shim_open_error`

Maps a `shim_open_exclusive` failure sentinel (a negative return code,
NOT an `IOReturn`) to its typed `Error` variant. Pulled out of `open`
as its own callable predicate so the mapping — otherwise reachable only
through a real IOKit FFI call — can be pinned by a test: collapsing
`-4..=-2` or `-5` into the `DeviceNotFound` catch-all would silently
turn "another process holds the drive" into "no such drive", or "the
IOKit plugin chain failed" into the same.

## Drive enumeration and the media-presence probe

`list_drives` uses the IOKit registry directly via `shim_list_drives` —
no exclusive access, no SCSI commands, no unmounts.

`drive_has_disc` is documented (`crate::scsi::drive_has_disc`) as the
cheap, side-effect-free "is there a disc?" question, suitable for a
poll-loop tick. The Linux backend honours that: `open(O_RDWR|O_NONBLOCK)`
+ one TEST UNIT READY, no exclusive access, no unmount. The Windows
backend likewise opens a shared handle and issues one TUR.

macOS could not do the same: `MacScsiTransport::open` is the FULL
exclusive-transport path above, whose first act is `diskutil unmountDisk
force` on the target device. So a probe implemented via `open` would
force-unmount the user's disc — and on every poll tick, taking (and
dropping) exclusive access each time.

Instead `drive_has_disc` answers via `shim_media_present`, the IOKit
registry only: steps 1-5 above are the *transport* open path, and this
probe must not run any of them. The registry answers the same question
with no side effect at all: the IOStorageFamily publishes an IOMedia
object for a removable device only while media is present and removes
it on eject, so a matching IOMedia is exactly "a disc is in the drive".
No SCSI command is issued, which is why no timeout parameter is
involved.

Trade-off, stated plainly: this reports what the OS has *enumerated*,
so a disc that is inserted but still spinning up (no IOMedia published
yet) reads as absent for the moment the enumeration takes — the same
window in which a TUR would answer "not ready" and this function's
contract already maps to `Ok(false)`.

## Regression test: `presence_probe_does_not_open_a_transport`

`drive_has_disc` used to be implemented by constructing a FULL exclusive
transport, whose first act is `diskutil unmountDisk force` on the target
device followed by an unconditional `usleep(500000)` — so the probe
force-unmounted the user's disc, on every poll tick.

Two observables separate the registry probe from the transport open,
neither of which needs an optical drive to be attached:

1. It ANSWERS. The transport path returned `Err(DeviceNotFound)` here;
   the registry path reports "no media" as `Ok(false)`.
2. It is FAST. The transport path's `usleep(500000)` after the spawn is
   unconditional, so it could not complete inside this budget even when
   the spawn itself failed.
