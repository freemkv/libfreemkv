//! Bit-exact CRC helpers shared by the audio codec decodability gates.
//!
//! Each matches the CRC defined by its format's bitstream specification, so a
//! frame these routines flag as a CRC mismatch is exactly the frame a
//! spec-conformant decoder would reject. All are MSB-first (non-reflected),
//! init 0, no final XOR — the big-endian CRC variants. Each format transmits
//! its CRC so that the residue over `data + transmitted_crc` is zero, which is
//! exactly how these are used: compute over the whole frame (including its
//! trailing CRC) and check `== 0`.

// CRC-16/ANSI (CRC-16/BUYPASS): poly 0x8005, init 0, MSB-first, no
// reflection, no final XOR. Used by AC-3/E-AC-3 frame-CRC (ETSI TS 102 366)
// and FLAC frame footer; MPEG-audio/AAC-ADTS don't verify their optional CRC.
pub(crate) fn crc16_ansi(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x8005
            } else {
                crc << 1
            };
        }
    }
    crc
}

// CRC-16, poly 0x002D, init 0, MSB-first — MLP/TrueHD major-sync checksum. Emits bytes in
// reversed order vs a standard little-endian CRC readout.
pub(crate) fn crc16_mlp(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x002D
            } else {
                crc << 1
            };
        }
    }
    crc
}

#[cfg(test)]
#[path = "crc_tests.rs"]
mod tests;
