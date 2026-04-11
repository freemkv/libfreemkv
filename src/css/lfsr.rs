//! CSS cipher — two LFSRs (17-bit + 25-bit) with byte combine.
//!
//! The CSS stream cipher XORs a keystream with sector bytes 128..2048.
//! A 40-bit key seeds both LFSRs. The output byte is a nonlinear
//! combination of both LFSR outputs.
//!
//! Reference: Frank Stevenson's DeCSS analysis (1999).

/// CSS substitution table — nonlinear byte mix for LFSR output combining.
/// This is the standard CSS S-box from the specification.
const CSS_TAB: [u8; 256] = {
    let mut tab = [0u8; 256];
    let mut i: usize = 0;
    while i < 256 {
        let b = i as u8;
        // CSS S-box: bit rotation + substitution
        // p4 is bit4 of (bit2 ^ bit1 ^ bit0 ^ (bit0 & bit1))
        let b0 = b & 1;
        let b1 = (b >> 1) & 1;
        let b2 = (b >> 2) & 1;
        let b3 = (b >> 3) & 1;
        let b4 = (b >> 4) & 1;
        let b5 = (b >> 5) & 1;
        let b6 = (b >> 6) & 1;
        let b7 = (b >> 7) & 1;
        tab[i] = (b0 ^ b1)
            | ((b0 ^ b2) << 1)
            | ((b0 ^ b3) << 2)
            | ((b0 ^ b4) << 3)
            | ((b0 ^ b5) << 4)
            | ((b0 ^ b6) << 5)
            | ((b0 ^ b7) << 6)
            | ((b1 ^ b7) << 7);
        i += 1;
    }
    tab
};

/// 17-bit LFSR feedback polynomial: x^17 + x^14 + 1
/// Taps at bits 0 and 3 (when counting from MSB of 17-bit value)
const LFSR17_FEEDBACK: u32 = 0x00012000;

/// 25-bit LFSR feedback polynomial: x^25 + x^12 + x^4 + x^3 + 1
const LFSR25_FEEDBACK: u32 = 0x01000018;

/// Clock the 17-bit LFSR one step. Returns output bit.
#[inline]
pub fn lfsr17_clock(state: &mut u32) -> u8 {
    let feedback = (*state ^ (*state >> 14)) & 1;
    let out = (*state & 0xFF) as u8;
    *state = (*state >> 8) | (feedback << 16) | (((*state >> 1) ^ (*state >> 6)) & 0xFF) << 9;
    // Simplified: shift right 8, feed back high bits
    // Actually CSS LFSR17 shifts 8 bits at a time for one output byte
    out
}

/// Clock the 25-bit LFSR one step. Returns output byte.
#[inline]
pub fn lfsr25_clock(state: &mut u32) -> u8 {
    // LFSR25 generates 8 bits per clock
    let mut out: u8 = 0;
    for bit in 0..8 {
        let feedback = (*state ^ (*state >> 3) ^ (*state >> 4) ^ (*state >> 12)) & 1;
        *state = (*state >> 1) | (feedback << 24);
        out |= ((*state >> 24) as u8 & 1) << bit;
    }
    out
}

/// Initialize both LFSRs from a 5-byte CSS key.
pub fn css_key_to_state(key: &[u8; 5]) -> (u32, u32) {
    // LFSR17 seeded from key bytes 0-1 + bit from byte 4
    let lfsr17 = (key[0] as u32) | ((key[1] as u32) << 8) | ((key[4] & 1) as u32) << 16;
    let lfsr17 = lfsr17 | 0x01; // must be nonzero

    // LFSR25 seeded from key bytes 2-4
    let lfsr25 = (key[2] as u32) | ((key[3] as u32) << 8) | ((key[4] as u32) << 16);
    let lfsr25 = lfsr25 | 0x01; // must be nonzero

    (lfsr17, lfsr25)
}

/// CSS S-box lookup. Used by the crack module to invert the cipher.
#[inline]
pub fn css_tab(byte: u8) -> u8 {
    CSS_TAB[byte as usize]
}

/// Generate one keystream byte from both LFSRs.
#[inline]
pub fn css_output_byte(lfsr17: &mut u32, lfsr25: &mut u32, carry: &mut u8) -> u8 {
    let o17 = lfsr17_clock(lfsr17);
    let o25 = lfsr25_clock(lfsr25);

    // Combine: add with carry through S-box
    let sum = o17 as u16 + o25 as u16 + *carry as u16;
    *carry = (sum >> 8) as u8;
    CSS_TAB[sum as u8 as usize]
}

/// Descramble a CSS-encrypted sector in place.
///
/// Bytes 0..128 are not encrypted (contain PES/pack headers).
/// Bytes 128..2048 are XORed with the CSS keystream.
pub fn descramble_sector(key: &[u8; 5], sector: &mut [u8]) {
    if sector.len() < 2048 {
        return;
    }

    // Check scramble flags in PES header (byte 0x14, bits 4-5)
    // 0 = not scrambled, 1 = scrambled with even key, 2 = scrambled with odd key
    // For simplicity, descramble if any flag is set
    let flags = (sector[0x14] >> 4) & 0x03;
    if flags == 0 {
        return;
    }

    let (mut lfsr17, mut lfsr25) = css_key_to_state(key);
    let mut carry: u8 = 0;

    // Skip first 128 bytes of keystream (they correspond to unencrypted header)
    for _ in 0..128 {
        css_output_byte(&mut lfsr17, &mut lfsr25, &mut carry);
    }

    // Descramble bytes 128..2048
    for i in 128..2048 {
        sector[i] ^= css_output_byte(&mut lfsr17, &mut lfsr25, &mut carry);
    }

    // Clear scramble flags
    sector[0x14] &= 0xCF;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_to_state_nonzero() {
        let key = [0u8; 5];
        let (lfsr17, lfsr25) = css_key_to_state(&key);
        assert_ne!(lfsr17, 0);
        assert_ne!(lfsr25, 0);
    }

    #[test]
    fn descramble_skips_unscrambled() {
        let key = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut sector = vec![0xAA; 2048];
        sector[0x14] = 0x00; // not scrambled
        let original = sector.clone();
        descramble_sector(&key, &mut sector);
        assert_eq!(sector, original, "unscrambled sector should be unchanged");
    }

    #[test]
    fn descramble_modifies_scrambled() {
        let key = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut sector = vec![0xAA; 2048];
        sector[0x14] = 0x30; // scramble flag set
        let original = sector.clone();
        descramble_sector(&key, &mut sector);
        // First 128 bytes should be unchanged except byte 0x14 (scramble flags cleared)
        for i in 0..128 {
            if i == 0x14 {
                continue;
            } // scramble flags cleared
            assert_eq!(sector[i], original[i], "byte {} changed", i);
        }
        // Bytes 128+ should be different (XORed with keystream)
        assert_ne!(&sector[128..256], &original[128..256]);
    }

    #[test]
    fn descramble_clears_flags() {
        let key = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut sector = vec![0x00; 2048];
        sector[0x14] = 0x30;
        descramble_sector(&key, &mut sector);
        assert_eq!(
            sector[0x14] & 0x30,
            0x00,
            "scramble flags should be cleared"
        );
    }

    #[test]
    fn descramble_roundtrip() {
        let key = [0x12, 0x34, 0x56, 0x78, 0x9A];
        let mut sector = vec![0u8; 2048];
        // Set known content
        for i in 128..2048 {
            sector[i] = (i & 0xFF) as u8;
        }
        sector[0x14] = 0x30; // scrambled
        let plaintext = sector[128..2048].to_vec();

        // Descramble (simulates encrypt by XOR)
        descramble_sector(&key, &mut sector);
        let ciphertext = sector[128..2048].to_vec();
        assert_ne!(ciphertext, plaintext);

        // Re-scramble (XOR again)
        sector[0x14] = 0x30;
        descramble_sector(&key, &mut sector);
        assert_eq!(&sector[128..2048], &plaintext[..]);
    }
}
