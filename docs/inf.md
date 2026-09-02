# `aacs::inf` — notes

## `UnitKeyFile`'s hand-written `Debug`

Redacting `Debug`, per the policy `aacs::types` documents: this struct holds
the disc's ENCRYPTED CPS unit keys — exactly the material a keydb entry stores
— plus the disc hash they are looked up by. A derived `Debug` printed every key
byte verbatim, so any `{:?}` (a downstream crate, an `assert_eq!` failure
message, a future `tracing::debug!` in this module) leaked them. Only
non-secret shape is printed. Guarded by `unit_key_file_debug_is_redacted`.

The disc hash is the public keydb lookup key, printed as hex the same way
`DiscEntry` prints its own — never as raw bytes.

## `parse_vtkf`

Layout — AACS "HD DVD and DVD Pre-recorded Book" Table 3-8, a fixed
2480-byte file, verified byte-exact against real discs (Freedom `VTKF090`,
Dukes of Hazzard `VTKF000`):
```text
  [0x00..0x0C] magic "DVD_HD_V_TKF"
  [0x0C..0x10] BE32 HD_VTKF_SIZE (2480)
  [0x10..0x1C] associated playlist name ("VPLST%%%.XPL")
  [0x1C..0x80] reserved
  [0x80..]     64 entries × 36 bytes:
                 BIFO (1) | reserved (3) | ENCRYPTED title key (16) | binding MAC (16)
                 BIFO bit 7 (AV_FLG) set = this slot holds a title key
                 (pre-recorded discs fill the binding MAC with 0xFF)
  [0x9A0..2480] 16-byte TKF MAC (CMAC keyed by Kvu — NOT a key)
```
The slot index (1-based) is the CPS unit number, so an absent slot is
SKIPPED (not a terminator) — collapsing gaps would renumber later keys and
hand the wrong title key to CPS unit N+1. The title→CPS mapping is
playlist-driven (`VPLST%%%.XPL`) and owned by the HD DVD enumerator, so
`title_cps_unit` is left empty here.

The prior parser used a 32-byte stride (a 12-byte pad instead of the 16-byte
binding MAC). That reads entry #1 correctly but drifts +4 bytes per entry
after it, so it only decrypted single-CPS-unit discs; every multi-key VTKF
(real multi-CPS-unit HD-DVD titles) yielded garbage keys for CPS unit ≥2.

## `synth_vtkf` test helper

Build a synthetic `VTKF%%%.AACS` matching the real on-disc layout (AACS
HD DVD Book Table 3-8, verified against Freedom `VTKF090` and Dukes
`VTKF000`): magic, BE32 size, playlist name, reserved to 0x80, then 64
entry slots of 36 bytes (the first `keys.len()` present with `AV_FLG`
set, the rest empty), a reserved gap, and the 16-byte trailing TKF MAC.

## `unit_key_file_debug_is_redacted` test

`UnitKeyFile` holds the disc's ENCRYPTED CPS unit keys. A derived `Debug`
printed every byte; the hand-written impl must not. Sentinel key byte
0xD5 = decimal 213 (a derived `Debug` renders `[u8; 16]` in decimal), the
same probe `aacs::types::redaction_tests` uses. Mutation guard: putting
`#[derive(Debug)]` back fails this.

## `read_mkb_from_drive_returns_the_concatenated_pack_payload` test

`read_mkb_from_drive` is the in-drive MKB source: every AACS derivation
downstream (`mkb_find_mk_dv`, the subset-difference walk, the whole
Media Key ladder) consumes exactly what it returns. An empty return is
not a benign "no MKB" — it is a total read failure reported as success,
and every derivation then fails with a key-not-found code that points
the operator at their keydb rather than at the drive.

This pins the CONTENT: the concatenated payload of all packs, in pack
order, byte for byte.

## `read_mkb_from_drive_issues_the_exact_mmc_cdb_for_each_pack` test

The CDB is what the drive actually acts on, and every byte of it is
load-bearing: a wrong format code returns a different disc structure
entirely, and a wrong allocation length truncates the pack. The existing
test above pins the opcode, the format code and the pack number; this
pins the WHOLE 12-byte CDB, so no field can drift unnoticed.

Expected layout (MMC-6 READ DISC STRUCTURE, AACS MKB format):
`[0]` opcode, `[1]` media type 0x01, `[2..6]` address = pack number
(BE32), `[6]` layer 0, `[7]` format 0x83, `[8..10]` allocation length
BE16 = 32772 = `0x80 0x04`, `[10..12]` reserved/control.

## `read_mkb_from_drive_accepts_a_full_size_pack` test

A pack payload filling the FULL 32768-byte window must come back whole.
The `len > 0 && len <= 32768` bound is what stands between a maximal
pack and a silently dropped one, and the small payloads used elsewhere
in this module never reach it.

## `read_mkb_from_drive_ignores_a_pack_declaring_more_than_the_buffer_holds` test

A drive that DECLARES more payload than it returned must not be
believed. The BE16 length in the response header is drive-supplied data:
a firmware bug, a short transfer, or a hostile device can put a value in
it that runs past the 32772-byte buffer. Copying `len` bytes on that word
alone panics the rip thread mid-scan.

Both the first-pack read and the per-pack loop carry the same bound, so
both are exercised here: the over-declared pack contributes nothing and
the honest pack still comes through.
