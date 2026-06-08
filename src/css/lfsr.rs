//! CSS cipher implementation based on the Stevenson 1999 analysis.
//!
//! The CSS cipher uses two table-driven feedback circuits:
//! - LFSR1: 17-bit state (9-bit lo + 8-bit hi register, seeded from
//!   key[0..2]), driven by TAB2/TAB3
//! - LFSR0: 32-bit state, driven by a feedback polynomial through TAB4
//!
//! The keystream is the bytewise sum (with carry) of both LFSR outputs.
//! Content descrambling XORs this keystream with the encrypted sector data.
//!
//! Algorithm: Frank A. Stevenson's divide-and-conquer attack (1999).
//! Tables: CSS specification constants.

use super::tables::{TAB1, TAB2, TAB3, TAB4, TAB5};

/// Seed the 32-bit LFSR0 register from the 5-byte working key, applying
/// the per-byte TAB4 bit-reversal. Shared by [`descramble_sector`] and
/// [`decrypt_key`] so the seeding lives in one place.
#[inline]
fn seed_lfsr0(key: &[u8; 5]) -> u32 {
    let lfsr0: u32 = ((key[4] as u32) << 17)
        | ((key[3] as u32) << 9)
        | (((key[2] as u32) << 1) + 8 - (key[2] as u32 & 7));
    (TAB4[(lfsr0 & 0xFF) as usize] as u32) << 24
        | (TAB4[((lfsr0 >> 8) & 0xFF) as usize] as u32) << 16
        | (TAB4[((lfsr0 >> 16) & 0xFF) as usize] as u32) << 8
        | TAB4[((lfsr0 >> 24) & 0xFF) as usize] as u32
}

/// One CSS keystream step. Advances both LFSRs, folds their permuted
/// outputs into `combined` (carry kept across calls), and returns the
/// low keystream byte. `invert` XORs the LFSR0 output index (0x00 on the
/// descramble path, 0xFF on the key-decrypt path).
#[inline]
fn css_step(
    lfsr1_lo: &mut u32,
    lfsr1_hi: &mut u32,
    lfsr0: &mut u32,
    combined: &mut u32,
    invert: u8,
) -> u8 {
    let o_lfsr1 = TAB2[*lfsr1_hi as usize] ^ TAB3[*lfsr1_lo as usize];
    *lfsr1_hi = *lfsr1_lo >> 1;
    *lfsr1_lo = ((*lfsr1_lo & 1) << 8) ^ o_lfsr1 as u32;

    let o_lfsr0 = (((((((*lfsr0 >> 8) ^ *lfsr0) >> 1) ^ *lfsr0) >> 3) ^ *lfsr0) >> 7) as u8;
    *lfsr0 = (*lfsr0 >> 8) | ((o_lfsr0 as u32) << 24);

    *combined += TAB5[o_lfsr1 as usize] as u32 + TAB4[(o_lfsr0 ^ invert) as usize] as u32;
    let out = (*combined & 0xFF) as u8;
    *combined >>= 8;
    out
}

/// Descramble a CSS-encrypted DVD sector in place.
///
/// The sector seed (bytes 0x54-0x58) is XORed with the title key to produce
/// the per-sector key. Bytes 0x80..0x800 (128..2048) are then decrypted
/// using the two-LFSR keystream.
///
/// The scramble flag at byte 0x14 (bits 4-5) indicates encryption.
/// After descrambling, the flag is cleared.
///
/// No-op (returns without modifying `sector`) in two cases:
/// - `sector.len() < 2048`: the encrypted region (0x80..0x800) is not
///   fully present. Callers chunk by 2048, so a trailing partial chunk is
///   left untouched. The `debug_assert!` flags this misuse in debug/test
///   builds; a DVD sector is always exactly 2048 bytes.
/// - scramble flags are zero: the sector is not CSS-encrypted.
pub fn descramble_sector(title_key: &[u8; 5], sector: &mut [u8]) {
    debug_assert!(
        sector.len() >= 2048,
        "descramble_sector: buffer shorter than one 2048-byte sector"
    );
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

    // Decrypt the key through the CSS mangling function to get the working key.
    // The sector seed is bytes 0x54..0x59 (5 bytes).
    let seed: [u8; 5] = [
        sector[0x54],
        sector[0x55],
        sector[0x56],
        sector[0x57],
        sector[0x58],
    ];
    let working_key = decrypt_key(0xFF, &key, &seed);

    // Generate keystream and XOR with encrypted region
    let mut lfsr1_lo: u32 = working_key[0] as u32 | 0x100;
    let mut lfsr1_hi: u32 = working_key[1] as u32;
    let mut lfsr0: u32 = seed_lfsr0(&working_key);

    let mut combined: u32 = 0;

    // Generate 1920 keystream bytes (for sector bytes 128..2048) and XOR them
    // into the encrypted region. Each keystream byte is the carrying sum of the
    // TAB5-permuted LFSR1 output and the TAB4-permuted LFSR0 output. No TAB1
    // permutation is applied to the ciphertext here (TAB1 is only used inside
    // decrypt_key); the working key was already produced by decrypt_key above,
    // so this keystream is paired with that mangling step, not a plain
    // direct-seed unscramble. No invert is applied on the LFSR0 output.
    for byte in sector.iter_mut().take(2048).skip(128) {
        let ks = css_step(
            &mut lfsr1_lo,
            &mut lfsr1_hi,
            &mut lfsr0,
            &mut combined,
            0x00,
        );
        *byte ^= ks;
    }

    // Clear scramble flags
    sector[0x14] &= 0xCF;
}

/// CSS key decryption / mangling function.
///
/// Decrypts `p_crypted` using `p_key` with the CSS two-LFSR cipher.
/// The `invert` parameter controls the XOR applied to LFSR0 output
/// (0x00 for disc key decryption, 0xFF for title key / sector key).
pub(crate) fn decrypt_key(invert: u8, p_key: &[u8; 5], p_crypted: &[u8; 5]) -> [u8; 5] {
    let mut lfsr1_lo: u32 = p_key[0] as u32 | 0x100;
    let mut lfsr1_hi: u32 = p_key[1] as u32;
    let mut lfsr0: u32 = seed_lfsr0(p_key);

    let mut combined: u32 = 0;
    let mut k = [0u8; 5];

    // TAB5 for LFSR1 output, TAB4 for LFSR0^invert (per libdvdcss css_DecryptKey).
    for byte in &mut k {
        *byte = css_step(
            &mut lfsr1_lo,
            &mut lfsr1_hi,
            &mut lfsr0,
            &mut combined,
            invert,
        );
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

    /// css_decrypt_key_roundtrip
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

    /// Test 2: descramble_modifies_encrypted_region
    ///
    /// descramble_sector XORs a keystream into bytes 128..2048. The keystream
    /// depends only on (title_key, sector_seed), so applying descramble twice
    /// with the scramble flag restored between calls re-XORs the same keystream
    /// and restores the original encrypted region — the keystream XOR is its
    /// own inverse. This pins the cipher's involution property over the body.
    #[test]
    fn css_descramble_modifies_encrypted_region() {
        let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];

        let mut sector = vec![0xAAu8; 2048];
        sector[0x14] = 0x30; // scramble flag
        sector[0x54..0x59].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x42]);

        let original = sector.clone();
        descramble_sector(&title_key, &mut sector);

        // Flag cleared
        assert_eq!(sector[0x14] & 0x30, 0x00);
        // Header (0..128) unchanged except flag byte
        for i in 0..128 {
            if i == 0x14 {
                continue;
            }
            assert_eq!(sector[i], original[i], "header byte {} changed", i);
        }
        // Encrypted region modified
        assert_ne!(&sector[128..256], &original[128..256]);

        // Round-trip: restore the scramble flag and descramble again. The same
        // keystream is regenerated (it depends only on title_key + seed, both
        // unchanged), so the body is restored to its original bytes.
        sector[0x14] = 0x30;
        descramble_sector(&title_key, &mut sector);
        assert_eq!(
            &sector[128..2048],
            &original[128..2048],
            "double descramble did not restore the encrypted region"
        );
    }

    /// css_tab1_relationship
    ///
    /// Verify the structure of TAB1: it is a substitution table used in
    /// key mangling. Check that no two inputs map to the same output
    /// (TAB1 is a permutation of 0..255).
    #[test]
    fn css_tab1_is_permutation() {
        let mut seen = [false; 256];
        for tab1_val in &TAB1 {
            let v = *tab1_val as usize;
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

    /// css_tab4_is_bit_reversal
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
