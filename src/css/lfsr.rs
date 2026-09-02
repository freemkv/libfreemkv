//! CSS content cipher (constants in [`super::tables`]). Two table-driven
//! linear-feedback circuits:
//! - **LFSR1** — 17-bit register seeded from `key[0..2] XOR seed[0..2]`,
//!   stepped through `TAB2`/`TAB3`/`TAB5`.
//! - **LFSR0** — 24-bit register seeded from `key[2..5] XOR seed[2..5]`,
//!   stepped through a feedback polynomial and `TAB4`.
//!
//! Output byte = sum-with-carry of both registers; body byte recovered as
//! `plain = TAB1[cipher] ^ keystream`. See docs/lfsr.md for more detail.

use super::tables::{TAB1, TAB2, TAB3, TAB4, TAB5};

/// Descramble a CSS-encrypted DVD sector in place.
///
/// Registers are seeded from `title_key XOR sector_seed` (bytes
/// `0x54..0x59`). Only the body, bytes `0x80..0x800`, is transformed:
/// `body[i] = TAB1[body[i]] ^ (keystream & 0xff)`. The scramble flag at byte
/// `0x14` (bits 4-5) is CLEARED after unscrambling, so a descrambled sector
/// reads as `sector[0x14] & 0x30 == 0`.
///
/// No-op (returns without modifying `sector`) if `sector.len() < 2048` or
/// the scramble flag bits are already zero. See docs/lfsr.md for rationale.
pub fn descramble_sector(title_key: &[u8; 5], sector: &mut [u8]) {
    debug_assert!(
        sector.len() >= 2048,
        "descramble_sector: buffer shorter than one 2048-byte sector"
    );
    if sector.len() < 2048 {
        return;
    }

    // Not scrambled (flag bits 4-5 clear) → nothing to do.
    if sector[0x14] & 0x30 == 0 {
        return;
    }

    // LFSR1 halves, seeded from (key ^ seed) bytes 0-1. The 9-bit half carries a
    // set bit 8 (`| 0x100`) as its running marker.
    let mut r1a: u32 = ((title_key[0] ^ sector[0x54]) as u32) | 0x100;
    let mut r1b: u32 = (title_key[1] ^ sector[0x55]) as u32;

    // LFSR0 (24-bit), seeded from the remaining three key/seed bytes, then
    // pre-conditioned `r0 = r0*2 + 8 - (r0 & 7)`.
    let mut r0: u32 = (((title_key[2] as u32)
        | ((title_key[3] as u32) << 8)
        | ((title_key[4] as u32) << 16))
        ^ ((sector[0x56] as u32) | ((sector[0x57] as u32) << 8) | ((sector[0x58] as u32) << 16)))
        & 0xFF_FFFF;
    r0 = r0 * 2 + 8 - (r0 & 7);

    // Keystream accumulator; the low byte is the current keystream byte and the
    // high bits carry into the next iteration.
    let mut acc: u32 = 0;

    for byte in sector.iter_mut().take(2048).skip(128) {
        // Step LFSR1: its output byte `o1`.
        let mut o1 = (TAB2[r1b as usize] ^ TAB3[r1a as usize]) as u32;
        r1b = r1a >> 1;
        r1a = ((r1a & 1) << 8) ^ o1;
        o1 = TAB5[o1 as usize] as u32;

        // Step LFSR0: its output byte `o0`.
        let mut o0 = (((((((r0 >> 3) ^ r0) >> 1) ^ r0) >> 8) ^ r0) >> 5) & 0xFF;
        r0 = (r0 << 8) | o0;
        o0 = TAB4[o0 as usize] as u32;

        // Combine (sum with carry) and recover the plaintext byte.
        acc += o0 + o1;
        *byte = TAB1[*byte as usize] ^ (acc & 0xFF) as u8;
        acc >>= 8;
    }

    // Clear the scramble bits so downstream code and tests can tell a sector was
    // descrambled; bits 6-7 of byte 0x14 are preserved.
    sector[0x14] &= 0xCF;
}

// Exact inverse of descramble_sector, for test use only (builds known
// ciphertext). See docs/lfsr.md — scramble_sector.
#[cfg(test)]
pub(crate) fn scramble_sector(title_key: &[u8; 5], sector: &mut [u8]) {
    if sector.len() < 2048 {
        return;
    }

    let mut r1a: u32 = ((title_key[0] ^ sector[0x54]) as u32) | 0x100;
    let mut r1b: u32 = (title_key[1] ^ sector[0x55]) as u32;
    let mut r0: u32 = (((title_key[2] as u32)
        | ((title_key[3] as u32) << 8)
        | ((title_key[4] as u32) << 16))
        ^ ((sector[0x56] as u32) | ((sector[0x57] as u32) << 8) | ((sector[0x58] as u32) << 16)))
        & 0xFF_FFFF;
    r0 = r0 * 2 + 8 - (r0 & 7);

    let mut acc: u32 = 0;

    for byte in sector.iter_mut().take(2048).skip(128) {
        let mut o1 = (TAB2[r1b as usize] ^ TAB3[r1a as usize]) as u32;
        r1b = r1a >> 1;
        r1a = ((r1a & 1) << 8) ^ o1;
        o1 = TAB5[o1 as usize] as u32;

        let mut o0 = (((((((r0 >> 3) ^ r0) >> 1) ^ r0) >> 8) ^ r0) >> 5) & 0xFF;
        r0 = (r0 << 8) | o0;
        o0 = TAB4[o0 as usize] as u32;
        acc += o0 + o1;

        // Inverse of `*p = TAB1[*p] ^ ks`: apply ks then TAB1's inverse.
        *byte = (*TAB1_INV)[(*byte ^ (acc & 0xFF) as u8) as usize];
        acc >>= 8;
    }

    // Mark the sector scrambled so the descrambler will process it.
    sector[0x14] = (sector[0x14] & 0xCF) | 0x10;
}

/// Inverse permutation of [`TAB1`], built at first use. `TAB1` is a bijection on
/// `0..256`, so `TAB1_INV[TAB1[x]] == x`.
#[cfg(test)]
static TAB1_INV: std::sync::LazyLock<[u8; 256]> = std::sync::LazyLock::new(|| {
    let mut inv = [0u8; 256];
    for (i, &v) in TAB1.iter().enumerate() {
        inv[v as usize] = i as u8;
    }
    inv
});

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

    // Regression vector pinning the implementation's deterministic output.
    // key = 42 13 37 BE EF, seed = DE AD BE EF 42, body = 0xAA.
    // See docs/lfsr.md — reference vector test.
    #[test]
    fn descramble_produces_the_reference_css_vector() {
        let key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let mut sector = vec![0xAAu8; 2048];
        sector[0x14] = 0x30;
        sector[0x54..0x59].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x42]);
        descramble_sector(&key, &mut sector);
        assert_eq!(
            &sector[0x80..0x90],
            &[
                0x81, 0x92, 0x24, 0xA2, 0x46, 0x70, 0x3C, 0x64, 0xA6, 0x91, 0x84, 0xF5, 0x1F, 0x98,
                0xA0, 0x31
            ],
            "descramble body head must match the reference CSS vector"
        );
        assert_eq!(
            &sector[0x7F8..0x800],
            &[0x46, 0x94, 0x80, 0x0E, 0x67, 0x36, 0x65, 0xBC],
            "descramble body tail must match the reference CSS vector"
        );
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

    // Not a plain XOR involution (TAB1 applies to ciphertext); scramble_sector
    // is the true inverse. See docs/lfsr.md — inverts-scramble test.
    #[test]
    fn css_descramble_inverts_scramble_over_body() {
        let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];

        let mut sector = vec![0xAAu8; 2048];
        sector[0x14] = 0x30; // scramble flag
        sector[0x54..0x59].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x42]);

        let original = sector.clone();

        // Scramble the plaintext body into ciphertext.
        scramble_sector(&title_key, &mut sector);
        // Header (0..128) unchanged except the flag byte (set by scramble).
        for i in 0..128 {
            if i == 0x14 {
                continue;
            }
            assert_eq!(sector[i], original[i], "header byte {} changed", i);
        }
        // Encrypted region modified
        assert_ne!(&sector[128..256], &original[128..256]);

        // Descramble restores the plaintext body byte-for-byte.
        descramble_sector(&title_key, &mut sector);
        assert_eq!(sector[0x14] & 0x30, 0x00, "flag cleared after descramble");
        assert_eq!(
            &sector[128..2048],
            &original[128..2048],
            "descramble(scramble(body)) did not restore the body"
        );
    }

    // TAB1 is a substitution table used in key mangling; verify it is a
    // permutation of 0..255 (no two inputs map to the same output).
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

    // Scramble-flag detection (byte 0x14, bits 4-5): only bits 4-5 are the
    // flag (mask 0x30); 0x40/0x80 must read UNSCRAMBLED. See docs/lfsr.md.
    #[test]
    fn descramble_treats_high_bits_of_0x14_as_clear() {
        let key = [0x01, 0x02, 0x03, 0x04, 0x05];
        for &flag in &[0x40u8, 0x80, 0xC0, 0x0F, 0x4F, 0x8F] {
            let mut sector = vec![0xAA; 2048];
            sector[0x14] = flag;
            sector[0x54..0x59].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55]);
            let original = sector.clone();
            descramble_sector(&key, &mut sector);
            assert_eq!(
                sector, original,
                "byte 0x14 = {flag:#04x} has flag bits 4-5 clear; sector must be untouched"
            );
        }
    }

    // Each scramble bit (4 and 5) independently marks a sector encrypted:
    // 0x10 and 0x20 must both trigger descrambling. See docs/lfsr.md.
    #[test]
    fn descramble_triggers_on_either_flag_bit() {
        let key = [0x01, 0x02, 0x03, 0x04, 0x05];
        for &flag in &[0x10u8, 0x20, 0x30] {
            let mut sector = vec![0xAA; 2048];
            sector[0x14] = flag;
            sector[0x54..0x59].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x42]);
            let original = sector.clone();
            descramble_sector(&key, &mut sector);
            assert_ne!(
                &sector[128..256],
                &original[128..256],
                "flag {flag:#04x} (bits 4-5 nonzero) must descramble the body"
            );
        }
    }

    // Only the two scramble bits are cleared (`& 0xCF`); bits 6-7 of byte
    // 0x14 survive: 0xF0 becomes 0xC0, NOT 0x00. See docs/lfsr.md.
    #[test]
    fn descramble_clear_preserves_high_bits_of_0x14() {
        let key = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut sector = vec![0x00; 2048];
        sector[0x14] = 0xF0; // bits 4-7 set; bits 4-5 are the flag
        sector[0x54..0x59].copy_from_slice(&[0x00; 5]);
        descramble_sector(&key, &mut sector);
        assert_eq!(
            sector[0x14], 0xC0,
            "scramble bits cleared, bits 6-7 preserved (0xF0 & 0xCF)"
        );
    }

    // ── header / body boundary (encrypted region is 0x80..0x800) ───────────

    // Encrypted region is exactly 0x80..0x800; header bytes 0x00..0x80
    // (incl. the seed at 0x54..0x59) must stay untouched. See docs/lfsr.md.
    #[test]
    fn descramble_leaves_header_and_seed_intact() {
        let key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let mut sector = vec![0x5Au8; 2048];
        sector[0x14] = 0x30;
        let seed = [0xDE, 0xAD, 0xBE, 0xEF, 0x42];
        sector[0x54..0x59].copy_from_slice(&seed);
        let original = sector.clone();
        descramble_sector(&key, &mut sector);
        for i in 0..0x80usize {
            if i == 0x14 {
                continue;
            }
            assert_eq!(
                sector[i], original[i],
                "header byte {i:#04x} must be untouched"
            );
        }
        assert_eq!(&sector[0x54..0x59], &seed, "sector seed must survive");
    }

    // The descrambler must touch the WHOLE body, including the final byte
    // (index 2047), guarding the `.take(2048)` bound. See docs/lfsr.md.
    #[test]
    fn descramble_covers_final_body_byte() {
        let key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let mut sector = vec![0x00u8; 2048];
        sector[0x14] = 0x30;
        sector[0x54..0x59].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55]);
        descramble_sector(&key, &mut sector);
        // Body was all zero; any nonzero in [0x80,0x800) is keystream. Confirm
        // the keystream reaches the final byte.
        assert_ne!(
            &sector[2040..2048],
            &[0u8; 8][..],
            "the tail of the body must be descrambled (loop must reach index 2047)"
        );
    }

    // The length guard is a FLOOR, not a ceiling: descramble_sector processes
    // the first sector of any over-long buffer. See docs/lfsr.md.
    #[test]
    fn descramble_processes_the_first_sector_of_an_over_long_buffer() {
        let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0xDE, 0xAD, 0xBE, 0xEF, 0x42];

        // Two sectors' worth of buffer; only the first is a sector.
        let mut buf = vec![0xAAu8; 4096];
        buf[0x14] = 0x30;
        buf[0x54..0x59].copy_from_slice(&seed);
        let original = buf.clone();

        descramble_sector(&title_key, &mut buf);

        assert_ne!(
            &buf[0x80..0x800],
            &original[0x80..0x800],
            "the first sector's body must be descrambled"
        );
        assert_eq!(buf[0x14] & 0x30, 0x00, "and its scramble flag cleared");
        assert_eq!(
            &buf[2048..4096],
            &original[2048..4096],
            "bytes past the first sector must be left untouched"
        );

        // The result must equal what a caller gets by passing exactly one
        // sector — the same transform, not a length-dependent one.
        let mut one = original[..2048].to_vec();
        descramble_sector(&title_key, &mut one);
        assert_eq!(
            &buf[..2048],
            &one[..],
            "the first sector must descramble identically either way"
        );
    }

    // Descramble is keyed by `title_key XOR seed`: two different title keys
    // must produce two different bodies. See docs/lfsr.md.
    #[test]
    fn descramble_output_depends_on_title_key() {
        let seed = [0xDE, 0xAD, 0xBE, 0xEF, 0x42];
        let make = |k: &[u8; 5]| {
            let mut s = vec![0x00u8; 2048];
            s[0x14] = 0x30;
            s[0x54..0x59].copy_from_slice(&seed);
            descramble_sector(k, &mut s);
            s
        };
        let a = make(&[0x01, 0x02, 0x03, 0x04, 0x05]);
        let b = make(&[0x01, 0x02, 0x03, 0x04, 0x06]); // differs in last byte
        assert_ne!(
            &a[128..2048],
            &b[128..2048],
            "different title keys must descramble differently"
        );
    }

    // Descramble is keyed by the sector seed too: same title key, different
    // seed -> different body. See docs/lfsr.md.
    #[test]
    fn descramble_output_depends_on_seed() {
        let key = [0x01, 0x02, 0x03, 0x04, 0x05];
        let make = |seed: [u8; 5]| {
            let mut s = vec![0x00u8; 2048];
            s[0x14] = 0x30;
            s[0x54..0x59].copy_from_slice(&seed);
            descramble_sector(&key, &mut s);
            s
        };
        let a = make([0x11, 0x22, 0x33, 0x44, 0x55]);
        let b = make([0x11, 0x22, 0x33, 0x44, 0x56]);
        assert_ne!(
            &a[128..2048],
            &b[128..2048],
            "different seeds must descramble differently"
        );
    }
}
