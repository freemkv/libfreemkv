# CRC helpers (`src/mux/codec/crc.rs`)

## `crc16_mlp` reversed byte order

MLP's checksum is the "reversed" scheme. `crc16_mlp` emits its two bytes in
the OPPOSITE order to a standard little-endian CRC readout, so the caller
swaps them back and compares against the stored trailer word read
LITTLE-endian — see `truehd::mlp_major_sync_crc_ok`, which is authoritative.

Comparing big-endian instead is precisely the bug that function was fixed
for: it could never validate a real extended major sync, so whole TrueHD
tracks were dropped silently. This comment used to prescribe exactly that,
and to point at a `truehd::mlp_major_sync_ok` that does not exist.
Verified against real MLP/TrueHD bitstreams (225/225 major-sync AUs).
