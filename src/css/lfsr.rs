//! CSS cipher implementation based on the Stevenson 1999 analysis.
//!
//! The CSS cipher uses two table-driven feedback circuits:
//! - LFSR1: 9-bit state (two halves), driven by TAB2/TAB3
//! - LFSR0: 32-bit state, driven by a feedback polynomial through TAB4
//!
//! The keystream is the bytewise sum (with carry) of both LFSR outputs.
//! Content descrambling XORs this keystream with the encrypted sector data.
//!
//! Algorithm: Frank A. Stevenson's divide-and-conquer attack (1999).
//! Tables: CSS specification constants.

use super::tables::{TAB1, TAB2, TAB3, TAB4};

/// Descramble a CSS-encrypted DVD sector in place.
///
/// The sector seed (bytes 0x54-0x58) is XORed with the title key to produce
/// the per-sector key. Bytes 0x80..0x800 (128..2048) are then decrypted
/// using the two-LFSR keystream.
///
/// The scramble flag at byte 0x14 (bits 4-5) indicates encryption.
/// After descrambling, the flag is cleared.
pub fn descramble_sector(title_key: &[u8; 5], sector: &mut [u8]) {
    if sector.len() < 2048 {
        return;
    }

    let flags = (sector[0x14] >> 4) & 0x03;
    if flags == 0 {
        return;
    }

    // Per-sector key = title_key XOR sector_seed (bytes 0x54-0x58)
    let key = [
        title_key[0] ^ sector[0x54],
        title_key[1] ^ sector[0x55],
        title_key[2] ^ sector[0x56],
        title_key[3] ^ sector[0x57],
        title_key[4] ^ sector[0x58],
    ];

    // Decrypt the key through the CSS mangling function to get the working key
    let working_key = decrypt_key(0xFF, &key, &sector[0x54..0x59]);

    // Generate keystream and XOR with encrypted region
    let mut lfsr1_lo: u32 = working_key[0] as u32 | 0x100;
    let mut lfsr1_hi: u32 = working_key[1] as u32;

    let mut lfsr0: u32 = ((working_key[4] as u32) << 17)
        | ((working_key[3] as u32) << 9)
        | ((working_key[2] as u32) << 1)
        + 8
        - (working_key[2] as u32 & 7);
    lfsr0 = (TAB4[(lfsr0 & 0xFF) as usize] as u32) << 24
        | (TAB4[((lfsr0 >> 8) & 0xFF) as usize] as u32) << 16
        | (TAB4[((lfsr0 >> 16) & 0xFF) as usize] as u32) << 8
        | TAB4[((lfsr0 >> 24) & 0xFF) as usize] as u32;

    let mut combined: u32 = 0;

    // Generate 1920 keystream bytes (for sector bytes 128..2048)
    for i in 128..2048 {
        // Clock LFSR1
        let o_lfsr1 = TAB2[lfsr1_hi as usize] ^ TAB3[lfsr1_lo as usize];
        lfsr1_hi = lfsr1_lo >> 1;
        lfsr1_lo = ((lfsr1_lo & 1) << 8) ^ o_lfsr1 as u32;
        let o_lfsr1_perm = TAB4[o_lfsr1 as usize];

        // Clock LFSR0
        let o_lfsr0 = (((((((lfsr0 >> 8) ^ lfsr0) >> 1) ^ lfsr0) >> 3) ^ lfsr0) >> 7) as u8;
        lfsr0 = (lfsr0 >> 8) | ((o_lfsr0 as u32) << 24);

        // Combine with addition and carry
        combined += (o_lfsr0 ^ 0xFF) as u32 + o_lfsr1_perm as u32;
        sector[i] ^= (combined & 0xFF) as u8;
        combined >>= 8;
    }

    // Clear scramble flags
    sector[0x14] &= 0xCF;
}

/// CSS key decryption / mangling function.
///
/// Decrypts `p_crypted` using `p_key` with the CSS two-LFSR cipher.
/// The `invert` parameter controls the XOR applied to LFSR0 output
/// (0x00 for disc key decryption, 0xFF for title key / sector key).
pub(crate) fn decrypt_key(
    invert: u8,
    p_key: &[u8; 5],
    p_crypted: &[u8],
) -> [u8; 5] {
    if p_crypted.len() < 5 {
        return *p_key;
    }

    let mut lfsr1_lo: u32 = p_key[0] as u32 | 0x100;
    let mut lfsr1_hi: u32 = p_key[1] as u32;

    let mut lfsr0: u32 = ((p_key[4] as u32) << 17)
        | ((p_key[3] as u32) << 9)
        | ((p_key[2] as u32) << 1)
        + 8
        - (p_key[2] as u32 & 7);
    lfsr0 = (TAB4[(lfsr0 & 0xFF) as usize] as u32) << 24
        | (TAB4[((lfsr0 >> 8) & 0xFF) as usize] as u32) << 16
        | (TAB4[((lfsr0 >> 16) & 0xFF) as usize] as u32) << 8
        | TAB4[((lfsr0 >> 24) & 0xFF) as usize] as u32;

    let mut combined: u32 = 0;
    let mut k = [0u8; 5];

    for i in 0..5 {
        let o_lfsr1 = TAB2[lfsr1_hi as usize] ^ TAB3[lfsr1_lo as usize];
        lfsr1_hi = lfsr1_lo >> 1;
        lfsr1_lo = ((lfsr1_lo & 1) << 8) ^ o_lfsr1 as u32;
        let o_lfsr1_perm = TAB4[o_lfsr1 as usize];

        let o_lfsr0 = (((((((lfsr0 >> 8) ^ lfsr0) >> 1) ^ lfsr0) >> 3) ^ lfsr0) >> 7) as u8;
        lfsr0 = (lfsr0 >> 8) | ((o_lfsr0 as u32) << 24);

        combined += (o_lfsr0 ^ invert) as u32 + o_lfsr1_perm as u32;
        k[i] = (combined & 0xFF) as u8;
        combined >>= 8;
    }

    // Two rounds of chained XOR through TAB1
    let mut result = [0u8; 5];
    result[4] = k[4] ^ TAB1[p_crypted[4] as usize] ^ p_crypted[3];
    result[3] = k[3] ^ TAB1[p_crypted[3] as usize] ^ p_crypted[2];
    result[2] = k[2] ^ TAB1[p_crypted[2] as usize] ^ p_crypted[1];
    result[1] = k[1] ^ TAB1[p_crypted[1] as usize] ^ p_crypted[0];
    result[0] = k[0] ^ TAB1[p_crypted[0] as usize] ^ result[4];

    result[4] = k[4] ^ TAB1[result[4] as usize] ^ result[3];
    result[3] = k[3] ^ TAB1[result[3] as usize] ^ result[2];
    result[2] = k[2] ^ TAB1[result[2] as usize] ^ result[1];
    result[1] = k[1] ^ TAB1[result[1] as usize] ^ result[0];
    result[0] = k[0] ^ TAB1[result[0] as usize];

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descramble_skips_unscrambled() {
        let key = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut sector = vec![0xAA; 2048];
        sector[0x14] = 0x00;
        let original = sector.clone();
        descramble_sector(&key, &mut sector);
        assert_eq!(sector, original);
    }

    #[test]
    fn descramble_modifies_scrambled() {
        let key = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut sector = vec![0xAA; 2048];
        sector[0x14] = 0x30; // scramble flag set
        // Set a sector seed
        sector[0x54..0x59].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55]);
        let original = sector.clone();
        descramble_sector(&key, &mut sector);
        // Header (0..128) unchanged except byte 0x14 (flag cleared)
        for i in 0..128 {
            if i == 0x14 {
                continue;
            }
            assert_eq!(sector[i], original[i], "header byte {} changed", i);
        }
        // Encrypted region should be different
        assert_ne!(&sector[128..256], &original[128..256]);
    }

    #[test]
    fn descramble_clears_flags() {
        let key = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut sector = vec![0x00; 2048];
        sector[0x14] = 0x30;
        sector[0x54..0x59].copy_from_slice(&[0x00; 5]);
        descramble_sector(&key, &mut sector);
        assert_eq!(sector[0x14] & 0x30, 0x00);
    }

    #[test]
    fn decrypt_key_produces_output() {
        let key = [0x12, 0x34, 0x56, 0x78, 0x9A];
        let crypted = [0xAB, 0xCD, 0xEF, 0x01, 0x23];
        let result = decrypt_key(0xFF, &key, &crypted);
        // Should produce a 5-byte result different from input
        assert_ne!(result, key);
        assert_ne!(result, [0u8; 5]);
    }
}
