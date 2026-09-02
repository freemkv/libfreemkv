# TrueHD codec parser — extended notes

## `is_truehd_major_sync` — why 0xBA must be exact

The 32-bit word that FOLLOWS the sync is laid out per stream type: TrueHD's
`format_info` carries [31..28] audio_sampling_frequency, the 5-bit 6-channel
and 13-bit 8-channel presentation channel-assignment masks; MLP's (0xBB) same
word carries quantization word lengths and the MLP group sample-rate fields
instead. So every site that decodes `format_info` with the TrueHD layout must
require 0xBA exactly — masking the low sync bit there read a quantization code
as the rate nibble and pulled channel masks out of unrelated bits, giving the
track header a wrong `SamplingFrequency` and `truehd_au_duration_ns` a wrong
per-AU increment (audio drifting against video for the whole track).
`None` from these helpers is the safe outcome: the caller falls back to its
container-derived rate/channel count.

## `truehd_sample_rate_hz` — rate formula and rationale

The MLP rate formula is `(ratebits & 8 ? 44100 : 48000) << (ratebits & 7)`;
rather than evaluate it blindly, `truehd_sample_rate_hz` is a strict
whitelist of the only six rates that occur on real BD/UHD TrueHD. Every
other code — the invalid `0xF`, the formula-only `0x3`/`0xB`, and all
reserved values — returns `None`.

## `au_check` — corruption decision

Decide whether an access unit is corrupt, updating `num_substreams` from a
valid major sync. Per the MLP/TrueHD access-unit decode rules: a major sync
with a bad header CRC, or any AU whose header parity fails, is undecodable.
Returns `false` (not corrupt) when the AU is too short to judge or no
major sync has established `num_substreams` yet — we never drop what we
cannot verify. Verified against real TrueHD streams (3600/3600 AUs).

## `ac3_frame_at_head` — AC-3 size lookup

Size (bytes) of the AC-3 frame at the buffer head. Distinguishes three cases
the caller must treat differently:
- `Unmappable`: the header's fscod/frmsizecod don't map to a real frame
  size (reserved fscod==3, or frmsizecod >= 38). The caller must drain
  and resync, NOT wait for more data — waiting would stall forever.
- `NeedMore`: a valid size, but the frame isn't fully buffered yet.
- `Frame(n)`: a complete `n`-byte AC-3 frame is buffered.

Frame sizing reuses `ac3::ac3_frame_size` so the AC-3 size table has a
single source of truth shared with the AC-3 parser; a returned `0` there
(reserved fscod or out-of-range frmsizecod) is the unmappable case.

## `ac3_boundary_corroborated`

Secondary validation for an AC-3 frame of `frame_bytes` at the buffer head:
is its computed end a plausible boundary? Accept when the frame fills the
rest of the buffer, or the bytes that follow start another AC-3 sync
(0x0B77) or a plausible TrueHD access unit (non-zero 12-bit length within
the 32 KiB cap). If none holds, the leading 0x0B77 is more likely a TrueHD
AU header that happens to look like AC-3, so the AC-3 reading is rejected.

## `mlp_major_sync_header_size`

Major-sync header size in bytes: base 28, plus `2 + extensions*2` when the
extension flag (major-sync byte 25, bit 0) is set (`extensions` = byte 26
high nibble). `ms` is the major-sync header, i.e. AU bytes `[4..]`. `None`
when the AU is too short to contain the full header.

## `mlp_major_sync_crc_ok`

Validate the MLP/TrueHD major-sync header checksum (a CRC-16 with polynomial
0x002D). The stored trailer is the last 2 header bytes; because MLP's
checksum is byte-reversed relative to a standard CRC, a standard CRC of the
header body XOR the little-endian word before the trailer must equal the
trailer read LITTLE-endian. (Comparing it big-endian was the bug this
function was fixed for; the body and the inline note are authoritative.)

## `mlp_substr_header_size` — test rationale

The substream count and the substream-directory size are the two numbers
that position `mlp_parity_ok`'s check window over the AU header. Every
other fixture in this module uses the degenerate shape — one substream,
no extraword — so neither function's real behaviour was ever exercised:
a constant answer agreed with all of them, and would then mis-window the
parity check on the multi-substream AUs that carry 7.1 and Atmos, judging
clean audio corrupt (or corrupt audio clean).

## `ref_crc16_2d` test oracle

Independent bitwise CRC-16 (poly 0x002D, init 0, MSB-first) — a separate
oracle from `crc16_mlp`, so a fixture built with it is not tautological
with the validator under test. Anchored to the catalogue check value
(0x4FF7 for "123456789") so the oracle itself is proven correct without
reference to the code under test.

## `finalize_major_sync` test helper

Turn a synthetic major-sync AU (sync bytes already set at offset 4, any
`format_info` set) into one that passes the decodability gate: 1 substream,
a clean substream directory, a valid major-sync CRC-16, and a valid header
parity nibble. Mirrors what a real encoder writes (verified against real
TrueHD streams). The AU must be ≥ 36 bytes (4 AU header + 28 major-sync
header + 2 directory + slack), which every `make_truehd_unit(≥200)` is.
