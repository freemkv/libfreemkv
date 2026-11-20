//! Bit-exact CRC helpers shared by the audio codec decodability gates.
//!
//! Each matches the CRC defined by its format's bitstream specification, so a
//! frame these routines flag as a CRC mismatch is exactly the frame a
//! spec-conformant decoder would reject. All are MSB-first (non-reflected),
//! init 0, no final XOR — the big-endian CRC variants. Each format transmits
//! its CRC so that the residue over `data + transmitted_crc` is zero, which is
//! exactly how these are used: compute over the whole frame (including its
//! trailing CRC) and check `== 0`.

// The 256-entry byte table for an MSB-first CRC-16 with polynomial `poly`: entry `i`
// is the CRC register after shifting `i << 8` through eight bit steps.
const fn crc16_table(poly: u16) -> [u16; 256] {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ poly
            } else {
                crc << 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC16_ANSI: [u16; 256] = crc16_table(0x8005);
static CRC16_MLP: [u16; 256] = crc16_table(0x002D);

// One table lookup per byte; the same register the bitwise loop computes.
fn crc16_with(table: &[u16; 256], data: &[u8]) -> u16 {
    data.iter().fold(0u16, |crc, &b| {
        (crc << 8) ^ table[usize::from((crc >> 8) as u8 ^ b)]
    })
}

// CRC-16/ANSI (CRC-16/BUYPASS): poly 0x8005, init 0, MSB-first, no
// reflection, no final XOR. Used by AC-3/E-AC-3 frame-CRC (ETSI TS 102 366)
// and FLAC frame footer; MPEG-audio/AAC-ADTS don't verify their optional CRC.
pub(crate) fn crc16_ansi(data: &[u8]) -> u16 {
    crc16_with(&CRC16_ANSI, data)
}

// CRC-16, poly 0x002D, init 0, MSB-first — MLP/TrueHD major-sync checksum. Emits bytes in
// reversed order vs a standard little-endian CRC readout.
pub(crate) fn crc16_mlp(data: &[u8]) -> u16 {
    crc16_with(&CRC16_MLP, data)
}

#[cfg(test)]
#[path = "crc_tests.rs"]
mod tests;
