# session.rs — rationale and design notes

Moved here from doc comments in `src/session.rs` to stay under the
comment-guard's per-item prose cap. See the pointers in that file for
which section applies to which item.

## Module overview

Lifecycle is intentionally SPLIT — `open` does transport mechanics only,
`identify` / `scan` are separate — so a consumer can fetch a poster off a
fast `identify` and update its UI before committing to a full `scan`.

libfreemkv resolves no keys and reads no keydb: the consumer builds the
host credentials / key-source layer (from `freemkv_keysources`) and hands
them in via `KeySpec`; the session merely forwards them into
`ScanOptions` at scan time. No cert derivation happens here.

## `resolve_keys_for`

Steps, identical to what the consumers did inline:
1. Take the disc's public AACS inputs (`Disc::inputs`); a non-AACS disc has
   none, so this is a no-op returning an empty trace and no fetch.
2. Sample up to `MIN_SAMPLE_UNITS` encrypted content units from the largest
   title via `reader` (`read_encrypted_units`) so a candidate key is
   validated against real ciphertext. Skipped (no wasted read) when the
   factory yields no sources — resolution is then a guaranteed miss anyway.
3. Run the ordered sources first-valid-wins (`resolve_and_apply_traced`),
   which banks the winning unit keys onto `disc`'s AACS state.
4. Build the read-time `KeyFetch` from the disc's inputs (its per-call
   samples are swapped in by the closure) using the same source factory.

The `reader` is whatever the disc lives behind — a live `Drive` or a
file-backed `SectorSource` from `scan_iso`; both implement `SectorSource`.

## `DiscSession::into_drive`

`Error::DeviceNotReady` is returned when the drive is no longer held — the
public `stage_drive_as_reader` moves it into the reader slot, and calling
this twice moves it out, so an empty slot is reachable through ordinary
use rather than being a caller error. A library must not panic from
public API, and a precondition that normal flow violates is a trap rather
than a contract.

## `DiscSession::from_parts_for_test`

Test-only constructor: build a session over an injected reader plus an
already-scanned disc without opening a live `Drive`. `DiscSession::open`
needs real hardware, so this is the only way to exercise the
`MuxInput::Session` mux arm (take_reader → resolve_inline_base_map →
DiscStream → with_key_map) and `resolve_keys`'s title-sampling branch
against a synthetic reader.

The drive slot stays `None` (a `MuxInput::Session` mux never touches it —
it reads through the staged `reader`); `device` carries a sentinel path so
the driver's missing-reader error still has a name.

`disc` is an `Option` so a test can construct a session that has not been
scanned (`None`) to exercise the `resolve_keys` "called before scan" guard.

## `scan_iso`

This is the file-backed counterpart to `DiscSession::scan`: it is the one
place that opens a `FileSectorSource`, reads its capacity, and runs
`Disc::scan_image`, so consumers (CLI, autorip) stop hand-rolling that
triple and stop constructing the low-level reader themselves. No SCSI, no
handshake, no key resolution — AACS resolution during the scan uses only
whatever `opts` already carries (mirroring how `Disc::scan_image` forwards
`ScanOptions`).

The returned reader is a fresh handle positioned at the start of the
image; callers that need to sample ciphertext (key resolution) or feed a
mux can reuse it directly rather than re-opening the file. `Disc::scan_image`
reads only through the same reader, and all reads are LBA-addressed, so
the handle is fully reusable afterward.

## `scan_dir`

The extra step over `scan_iso` is the encryption verdict. `Disc::scan_with`
decides `encrypted` structurally, from the presence of an `/AACS` or
`/BDMV/AACS` directory (see `disc::aacs_dir_present`). For the common
case — a typical disc backup, which strips `AACS/` — that already gives
the right answer, and `DecryptKeys::None` is a pass-through. But a folder
copied verbatim from a decrypted disc keeps `AACS/`, and the tree shape
then claims encryption over content that is already in the clear: the rip
would fail asking for a key it does not need.

So for a folder, tree shape is not the evidence — content is. Several
aligned units at the largest title's start are sampled and judged by
`aacs_unit_needs_decrypt`, the same authority the mux read path uses:

* none need decryption → the folder is decrypted; `encrypted` is forced
  false and the reason is logged.
* any unit does → the folder is a raw encrypted copy, which `dir://` does
  not support; `Error::DirImageEncrypted`.

This lives here and not in `Disc::scan_image`, which is shared with the
ISO and drive paths: an ISO that carries `AACS/` and clear content is a
different situation (it may be mid-decrypt, or `--raw` output), and the
verdict must not change underneath those callers.

## `apply_folder_encryption_verdict`

Shared by `scan_dir` and by the `dir://` PES input path in `mux::resolve`.
It lives in one place because the two disagreed: a folder that ripped
through `scan_dir` failed through `input()`, which is the exact failure
this probe was written to prevent, reachable by the other door.
