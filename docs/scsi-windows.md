# Windows SCSI transport (`src/scsi/windows.rs`)

## `IOCTL_STORAGE_RESET_DEVICE` derivation

`IOCTL_STORAGE_RESET_DEVICE` (ntddstor.h) = `CTL_CODE(IOCTL_STORAGE_BASE=0x2D,
0x0401, METHOD_BUFFERED=0, FILE_READ_ACCESS=1)` =
`(0x2D<<16) | (1<<14) | (0x0401<<2) | 0` = `0x002D0000 | 0x4000 | 0x1004` =
`0x002D5004`.

Two earlier values were wrong: `0x002D1004` (function `0x401` but access
bits cleared) and `0x002DD000` (function `0x400` + R|W access — the
OBSOLETE RESET_BUS code class drivers reject). Both made `DeviceIoControl`
fail `ERROR_INVALID_FUNCTION`, silently skipping the reset. See the
value-regression test at the bottom of the module.

## `StorageAdapterDescriptor` field layout

Subset of `STORAGE_ADAPTER_DESCRIPTOR` (winioctl.h) up to and including
`MaximumTransferLength`. The real struct has more trailing fields, but the
driver fills the whole thing and we only read this prefix; reading a
truncated descriptor is the documented usage. Field layout (all the
leading fields are present so the offset of `MaximumTransferLength` is
correct): `Version, Size, MaximumTransferLength, MaximumPhysicalPages,
AlignmentMask: u32 …`.

`STORAGE_BUS_TYPE` is an `int`-sized enum (4 bytes), not a byte: with the
preceding BOOLEANs filling 20..24, `BusType` sits at offset 24 and the
USHORT version fields follow at 28/30, matching winioctl.h. A previous `u8`
declaration for `BusType` kept the total size at 32 by coincidence but
shifted `BusMajorVersion`/`BusMinorVersion` to offsets 26/28 (vs the SDK's
28/30), so any reader of those fields got wrong values. The layout test
below asserts every offset field-for-field against the SDK.

## `alignment_mask` / bounce-buffer rationale

Adapter `AlignmentMask` (`STORAGE_ADAPTER_DESCRIPTOR`, ntddscsi.h /
winioctl.h), queried alongside `max_transfer`. It is a *mask*: `0` (the
common case on USB optical bridges) means the DataBuffer may sit at any
address; `3` means DWORD-aligned, `7` 8-byte, etc. — always one less than
the required alignment. SCSI/SAS HBAs report nonzero masks, and
`IOCTL_SCSI_PASS_THROUGH_DIRECT` rejects a misaligned `DataBuffer`
(`DeviceIoControl` fails → all reads return transport failure / status
0xFF). When set and the caller's buffer is misaligned, `execute()` bounces
through an aligned scratch buffer.

## `normalize_device_path` duplication

A near-identical `normalize_path` exists in `drive::windows`. Both are kept
because they live in separate `cfg(windows)` modules that cannot easily
share a helper without introducing cross-module coupling.

## `query_adapter_descriptor` contract

Queries the storage adapter descriptor via `IOCTL_STORAGE_QUERY_PROPERTY` /
`StorageAdapterProperty` and returns `(max_transfer_bytes, alignment_mask)`.

`max_transfer_bytes`: the adapter's `MaximumTransferLength`. On any failure
(IOCTL failed, short reply, or a nonsensical zero) falls back to the
conservative `WINDOWS_MIN_TRANSFER_BYTES`; otherwise clamped up to that
floor. Never 0.

`alignment_mask`: the adapter's `AlignmentMask` (offset 16 in
`STORAGE_ADAPTER_DESCRIPTOR`). `0` means no alignment requirement. If the
reply is too short to include `AlignmentMask`, returns `0` (no
requirement) — the safe default, since any address satisfies a zero mask
and the descriptor's leading fields are read first regardless.

## `ScsiPassThroughDirect` layout, cross-checked against SDK

Cross-checked against the authoritative `SCSI_PASS_THROUGH_DIRECT` in the
Windows SDK `ntddscsi.h`. That struct has **no `#pragma pack`** — it uses
natural alignment — so on 64-bit Windows (LLP64, 8-byte `PVOID`) the
compiler pads `DataBuffer` to offset 24 and the struct is 56 bytes. That is
the layout `DeviceIoControl` expects, and bare `#[repr(C)]` reproduces it.

Do NOT add `packed(4)`: that yields offset 20 / 48 bytes, which is the
SDK's SEPARATE 32-bit thunk struct `SCSI_PASS_THROUGH_DIRECT32` (`VOID*
POINTER_32 DataBuffer`). Using that 32-bit layout on a 64-bit host malforms
every SPTI ioctl, so INQUIRY fails and drive enumeration returns zero
drives (the rc.4 Windows "no drives detected" regression).

## `windows_ffi_constants_match_sdk` test rationale

Each IOCTL is asserted against an INDEPENDENT re-derivation of the
`CTL_CODE` macro (devioctl.h:
`(DeviceType<<16) | (Access<<14) | (Function<<2) | Method`), not just its
literal — so a mistyped constant fails the derivation, not a tautology.
