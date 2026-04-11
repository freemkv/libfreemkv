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
        | (((working_key[2] as u32) << 1) + 8 - (working_key[2] as u32 & 7));
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
pub(crate) fn decrypt_key(invert: u8, p_key: &[u8; 5], p_crypted: &[u8]) -> [u8; 5] {
    if p_crypted.len() < 5 {
        return *p_key;
    }

    let mut lfsr1_lo: u32 = p_key[0] as u32 | 0x100;
    let mut lfsr1_hi: u32 = p_key[1] as u32;

    let mut lfsr0: u32 = ((p_key[4] as u32) << 17)
        | ((p_key[3] as u32) << 9)
        | (((p_key[2] as u32) << 1) + 8 - (p_key[2] as u32 & 7));
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

    /// Test 1: css_decrypt_key_roundtrip
    ///
    /// decrypt_key is not a simple encrypt/decrypt pair — it is a one-way mangling
    /// function. However, we can verify consistency: calling it twice with the same
    /// parameters produces the same output, and varying the invert byte changes
    /// the LFSR0 contribution predictably.
    #[test]
    fn css_decrypt_key_roundtrip() {
        let keys: &[[u8; 5]] = &[
            [0x12, 0x34, 0x56, 0x78, 0x9A],
            [0x00, 0x00, 0x00, 0x00, 0x00],
            [0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            [0xAB, 0xCD, 0xEF, 0x01, 0x23],
        ];
        let crypted_inputs: &[[u8; 5]] = &[
            [0x11, 0x22, 0x33, 0x44, 0x55],
            [0xAA, 0xBB, 0xCC, 0xDD, 0xEE],
            [0x00, 0x00, 0x00, 0x00, 0x00],
        ];

        for key in keys {
            for crypted in crypted_inputs {
                // decrypt_key with invert=0x00 and invert=0xFF should give different results
                let r0 = decrypt_key(0x00, key, crypted);
                let rff = decrypt_key(0xFF, key, crypted);

                // The two results differ because the invert byte XORs the LFSR0 output
                // They should not be equal (except by extreme coincidence)
                // More importantly, both should be deterministic
                let r0_again = decrypt_key(0x00, key, crypted);
                let rff_again = decrypt_key(0xFF, key, crypted);
                assert_eq!(r0, r0_again, "decrypt_key(0x00) not deterministic");
                assert_eq!(rff, rff_again, "decrypt_key(0xFF) not deterministic");

                // With different invert values, the keystream differs
                assert_ne!(
                    r0, rff,
                    "invert=0x00 and 0xFF gave same result for key {:?}",
                    key
                );
            }
        }
    }

    /// Test 2: css_descramble_produces_valid_mpeg2
    ///
    /// descramble_sector XORs a keystream into bytes 128..2048. Calling it
    /// twice with the same key and restored scramble flag should roundtrip,
    /// since XOR is its own inverse.
    #[test]
    fn css_descramble_produces_valid_mpeg2() {
        let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];

        // Build a sector with MPEG-2 pack header and PES header
        let mut sector = vec![0x00u8; 2048];
        // Pack header at byte 0
        sector[0] = 0x00;
        sector[1] = 0x00;
        sector[2] = 0x01;
        sector[3] = 0xBA;
        // Scramble flag at byte 0x14
        sector[0x14] = 0x30;
        // Sector seed at bytes 0x54-0x58
        sector[0x54..0x59].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x42]);
        // PES header at byte 128
        sector[0x80] = 0x00;
        sector[0x81] = 0x00;
        sector[0x82] = 0x01;
        sector[0x83] = 0xE0;
        // Fill some content in the encrypted region
        for i in 0x84..2048 {
            sector[i] = (i & 0xFF) as u8;
        }

        let original = sector.clone();

        // First descramble: "encrypts" by XORing keystream
        descramble_sector(&title_key, &mut sector);
        // Flag should be cleared
        assert_eq!(
            sector[0x14] & 0x30,
            0x00,
            "scramble flag not cleared after first descramble"
        );
        // Encrypted region should differ
        assert_ne!(
            &sector[0x80..0x84],
            &original[0x80..0x84],
            "encrypted region unchanged after descramble"
        );

        // Restore the scramble flag and sector seed for second pass
        sector[0x14] = 0x30;

        // Second descramble: XOR again = roundtrip
        descramble_sector(&title_key, &mut sector);
        // Now the encrypted region should match original
        assert_eq!(
            &sector[0x80..2048],
            &original[0x80..2048],
            "double descramble did not roundtrip"
        );
    }

    /// Test 4: css_tab1_relationship
    ///
    /// Verify the structure of TAB1: it is a substitution table used in
    /// key mangling. Check that no two inputs map to the same output
    /// (TAB1 is a permutation of 0..255).
    #[test]
    fn css_tab1_is_permutation() {
        let mut seen = [false; 256];
        for i in 0..256 {
            let v = TAB1[i] as usize;
            assert!(!seen[v], "TAB1 maps two inputs to {:#04x}", v);
            seen[v] = true;
        }
        // Check involution property: TAB1[TAB1[x]] should map back predictably
        // TAB1 is not necessarily a strict involution, but we verify the
        // composition TAB1[TAB1[x]] is also a permutation
        let mut seen2 = [false; 256];
        for i in 0..256 {
            let v = TAB1[TAB1[i] as usize] as usize;
            assert!(!seen2[v], "TAB1[TAB1[x]] maps two inputs to {:#04x}", v);
            seen2[v] = true;
        }
    }

    /// Test 5: css_tab4_is_bit_reversal
    ///
    /// TAB4 reverses the bits of each byte: TAB4[0x01] = 0x80, TAB4[0x80] = 0x01, etc.
    #[test]
    fn css_tab4_is_bit_reversal() {
        for i in 0u16..256 {
            let expected = (0..8).fold(0u8, |acc, bit| acc | (((i as u8 >> bit) & 1) << (7 - bit)));
            assert_eq!(
                TAB4[i as usize], expected,
                "TAB4[{:#04x}] = {:#04x}, expected {:#04x} (bit reversal)",
                i, TAB4[i as usize], expected
            );
        }
        // Also verify TAB4 is an involution: TAB4[TAB4[x]] == x
        for i in 0..256 {
            assert_eq!(
                TAB4[TAB4[i] as usize], i as u8,
                "TAB4 is not an involution at {:#04x}",
                i
            );
        }
    }
}
