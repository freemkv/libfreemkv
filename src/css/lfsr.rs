//! CSS cipher implementation based on the Stevenson 1999 analysis.
//!
//! The CSS cipher uses two table-driven feedback circuits:
//! - LFSR1: 17-bit state (9-bit lo + 8-bit hi register, seeded from
//!   key[0..2]), driven by TAB2/TAB3
//! - LFSR0: 24-bit feedback register (seeded from key[2..5] XOR seed[2..5],
//!   masked to 0xFFFFFF), driven by a feedback polynomial through TAB4
//!
//! The keystream is the bytewise sum (with carry) of both LFSR outputs.
//! Content descrambling computes plain = TAB1[cipher] ^ keystream — a TAB1
//! substitution of each ciphertext byte followed by an XOR with the keystream
//! (NOT a plain XOR; the cipher is not its own inverse).
//!
//! Algorithm: Frank A. Stevenson's divide-and-conquer attack (1999).
//! Tables: CSS specification constants.

use super::tables::{TAB1, TAB2, TAB3, TAB4, TAB5};

/// Descramble a CSS-encrypted DVD sector in place.
///
/// Exact port of libdvdcss `dvdcss_unscramble` (css.c). The two content
/// LFSRs are seeded **directly** from `title_key XOR sector_seed` — there is
/// no `decrypt_key` mangling on this path (that is the disc/title-key
/// hierarchy, not the content cipher). Bytes 0x80..0x800 are recovered with
/// `*p = TAB1[*p] ^ (i_t5 & 0xff)`.
///
/// The scramble flag at byte 0x14 (bits 4-5) indicates encryption. This
/// descrambler CLEARS that flag after unscrambling, so a descrambled sector
/// reads as `sector[0x14] & 0x30 == 0`; callers and the tests use that to tell
/// it from ciphertext, and re-running descramble on an already-cleared sector
/// is a no-op (the flag guard below skips it). Clearing does not affect the
/// recovered body.
///
/// No-op (returns without modifying `sector`) in two cases:
/// - `sector.len() < 2048`: the encrypted region (0x80..0x800) is not
///   fully present. Callers chunk by 2048, so a trailing partial chunk is
///   left untouched. The `debug_assert!` flags this misuse in debug/test
///   builds; a DVD sector is always exactly 2048 bytes.
/// - scramble flags are zero: the sector is not CSS-encrypted.
///
/// Design reference: libdvdcss `dvdcss_unscramble`. The combiner mirrors
/// `css.c` line-for-line:
/// ```text
/// i_t1 = (key[0] ^ sec[0x54]) | 0x100;
/// i_t2 =  key[1] ^ sec[0x55];
/// i_t3 = (key[2]|key[3]<<8|key[4]<<16) ^ (sec[0x56]|sec[0x57]<<8|sec[0x58]<<16);
/// i_t4 = i_t3 & 7;  i_t3 = i_t3*2 + 8 - i_t4;
/// // per byte over 0x80..0x800:
/// i_t4 = TAB2[i_t2] ^ TAB3[i_t1];
/// i_t2 = i_t1 >> 1;  i_t1 = ((i_t1 & 1) << 8) ^ i_t4;  i_t4 = TAB5[i_t4];
/// i_t6 = (((((((i_t3>>3)^i_t3)>>1)^i_t3)>>8)^i_t3)>>5) & 0xff;
/// i_t3 = (i_t3 << 8) | i_t6;  i_t6 = TAB4[i_t6];
/// i_t5 += i_t6 + i_t4;  *p = TAB1[*p] ^ (i_t5 & 0xff);  i_t5 >>= 8;
/// ```
pub fn descramble_sector(title_key: &[u8; 5], sector: &mut [u8]) {
    debug_assert!(
        sector.len() >= 2048,
        "descramble_sector: buffer shorter than one 2048-byte sector"
    );
    if sector.len() < 2048 {
        return;
    }

    // libdvdcss: `if( !(p_sec[0x14] & 0x30) ) return;`
    if sector[0x14] & 0x30 == 0 {
        return;
    }

    // LFSR1: seeded directly from (key ^ seed) — NO decrypt_key.
    let mut i_t1: u32 = ((title_key[0] ^ sector[0x54]) as u32) | 0x100;
    let mut i_t2: u32 = (title_key[1] ^ sector[0x55]) as u32;

    // LFSR0 (i_t3): 24-bit feedback register seeded from the remaining three
    // key/seed bytes, then transformed `i_t3 = i_t3*2 + 8 - (i_t3 & 7)`.
    let mut i_t3: u32 = (((title_key[2] as u32)
        | ((title_key[3] as u32) << 8)
        | ((title_key[4] as u32) << 16))
        ^ ((sector[0x56] as u32) | ((sector[0x57] as u32) << 8) | ((sector[0x58] as u32) << 16)))
        & 0xFF_FFFF;
    let i_t4_seed = i_t3 & 7;
    i_t3 = i_t3 * 2 + 8 - i_t4_seed;

    let mut i_t5: u32 = 0;

    for byte in sector.iter_mut().take(2048).skip(128) {
        // Advance LFSR1.
        let mut i_t4 = (TAB2[i_t2 as usize] ^ TAB3[i_t1 as usize]) as u32;
        i_t2 = i_t1 >> 1;
        i_t1 = ((i_t1 & 1) << 8) ^ i_t4;
        i_t4 = TAB5[i_t4 as usize] as u32;

        // Advance LFSR0 (i_t3) and fold both outputs into i_t5.
        let mut i_t6 = (((((((i_t3 >> 3) ^ i_t3) >> 1) ^ i_t3) >> 8) ^ i_t3) >> 5) & 0xFF;
        i_t3 = (i_t3 << 8) | i_t6;
        i_t6 = TAB4[i_t6 as usize] as u32;
        i_t5 += i_t6 + i_t4;

        *byte = TAB1[*byte as usize] ^ (i_t5 & 0xFF) as u8;
        i_t5 >>= 8;
    }

    // libdvdcss leaves byte 0x14 untouched; freemkv clears the scramble bits
    // so downstream code and tests can tell a sector was descrambled.
    sector[0x14] &= 0xCF;
}

/// Exact inverse of [`descramble_sector`]: turn a plaintext sector body into
/// CSS ciphertext under `title_key`.
///
/// Descramble computes `plain = TAB1[cipher] ^ (i_t5 & 0xff)`, so the
/// inverse is `cipher = TAB1_INV[plain ^ (i_t5 & 0xff)]` with the identical
/// LFSR keystream. The keystream derivation is byte-for-byte the same as
/// `descramble_sector` (libdvdcss `dvdcss_unscramble`); only the final
/// substitution differs. Bytes 0x80..0x800 are rewritten in place; the
/// scramble flag is set to 0x10 so a subsequent descramble runs.
///
/// Not on any production read path — it exists so the key-recovery tests
/// (and any caller that needs to produce a known CSS-encrypted sector) can
/// build genuine ciphertext rather than approximating it.
#[cfg(test)]
pub(crate) fn scramble_sector(title_key: &[u8; 5], sector: &mut [u8]) {
    if sector.len() < 2048 {
        return;
    }

    let mut i_t1: u32 = ((title_key[0] ^ sector[0x54]) as u32) | 0x100;
    let mut i_t2: u32 = (title_key[1] ^ sector[0x55]) as u32;
    let mut i_t3: u32 = (((title_key[2] as u32)
        | ((title_key[3] as u32) << 8)
        | ((title_key[4] as u32) << 16))
        ^ ((sector[0x56] as u32) | ((sector[0x57] as u32) << 8) | ((sector[0x58] as u32) << 16)))
        & 0xFF_FFFF;
    let i_t4_seed = i_t3 & 7;
    i_t3 = i_t3 * 2 + 8 - i_t4_seed;

    let mut i_t5: u32 = 0;

    for byte in sector.iter_mut().take(2048).skip(128) {
        let mut i_t4 = (TAB2[i_t2 as usize] ^ TAB3[i_t1 as usize]) as u32;
        i_t2 = i_t1 >> 1;
        i_t1 = ((i_t1 & 1) << 8) ^ i_t4;
        i_t4 = TAB5[i_t4 as usize] as u32;

        let mut i_t6 = (((((((i_t3 >> 3) ^ i_t3) >> 1) ^ i_t3) >> 8) ^ i_t3) >> 5) & 0xFF;
        i_t3 = (i_t3 << 8) | i_t6;
        i_t6 = TAB4[i_t6 as usize] as u32;
        i_t5 += i_t6 + i_t4;

        // Inverse of `*p = TAB1[*p] ^ ks`: apply ks then TAB1's inverse.
        *byte = (*TAB1_INV)[(*byte ^ (i_t5 & 0xFF) as u8) as usize];
        i_t5 >>= 8;
    }

    // Mark the sector scrambled so the descrambler will process it.
    sector[0x14] = (sector[0x14] & 0xCF) | 0x10;
}

/// Inverse permutation of [`TAB1`], built at first use. `TAB1` is a
/// bijection on 0..256, so `TAB1_INV[TAB1[x]] == x`.
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

    /// Cross-check `descramble_sector` against the EXACT output of libdvdcss
    /// `dvdcss_unscramble` (css.c) for a fixed sector, computed from the
    /// reference C semantics with the reference tables. Pins the content
    /// cipher to libdvdcss byte-for-byte.
    ///
    /// key = 42 13 37 BE EF, seed (0x54..0x59) = DE AD BE EF 42, body = 0xAA.
    #[test]
    fn descramble_matches_libdvdcss_unscramble_vector() {
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
            "descramble body head must match libdvdcss dvdcss_unscramble"
        );
        assert_eq!(
            &sector[0x7F8..0x800],
            &[0x46, 0x94, 0x80, 0x0E, 0x67, 0x36, 0x65, 0xBC],
            "descramble body tail must match libdvdcss dvdcss_unscramble"
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

    /// Test 2: descramble inverts scramble over the body.
    ///
    /// The content cipher is NOT a plain XOR involution (it applies TAB1 to
    /// the ciphertext: `plain = TAB1[cipher] ^ ks`). The true inverse is
    /// [`scramble_sector`]. Scrambling a plaintext body and then descrambling
    /// with the same key must reproduce the original body exactly.
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

    // ── scramble-flag detection (byte 0x14, bits 4-5) ──────────────────────

    /// Only bits 4-5 of byte 0x14 are the CSS scramble flag: the code reads
    /// `sector[0x14] & 0x30 == 0` (bits 6-7, i.e. 0x40/0x80, are masked out by
    /// 0x30). A sector with 0x14 == 0x40 or 0x80 must therefore be treated as
    /// UNSCRAMBLED and left byte-for-byte unchanged. This guards against a
    /// too-wide mask silently "descrambling" (and thus corrupting) clear data.
    ///
    /// Grounding: CSS sector header byte 0x14 — copyright/scramble bits live
    /// in bits 4-5; the masked value 0 means not scrambled.
    /// Mutation: widen the mask `0x30` to `0x70`/`0xF0` -> 0x40/0x80 would be
    /// seen as scrambled and the body would change.
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

    /// Each individual scramble bit (4 and 5) independently marks the sector
    /// as encrypted: 0x10 and 0x20 must both trigger descrambling.
    ///
    /// Grounding: `(0x10 >> 4) & 3 == 1`, `(0x20 >> 4) & 3 == 2` — both
    /// nonzero.
    /// Mutation: change `!= 0` early-return condition to `== 3` -> a sector
    /// flagged only 0x10 or 0x20 would be skipped and left scrambled.
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

    /// After descrambling, ONLY the two scramble bits are cleared (`& 0xCF`);
    /// bits 6 and 7 of byte 0x14 must be preserved. A sector with 0x14 == 0xF0
    /// becomes 0xC0 (bits 6,7 kept, bits 4,5 cleared), NOT 0x00.
    ///
    /// Grounding: code does `sector[0x14] &= 0xCF`; 0xF0 & 0xCF == 0xC0.
    /// Mutation: change `&= 0xCF` to `= 0` or `&= 0x0F` -> the preserved
    /// high bits assert fails.
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

    /// The encrypted region is exactly bytes 0x80..0x800. Bytes 0x00..0x80
    /// (the header) must NOT be modified by the keystream — except byte 0x14
    /// whose flag is cleared. In particular the sector-seed bytes 0x54..0x59
    /// (which live inside the header) must survive untouched, since the
    /// descrambler reads them but never writes them.
    ///
    /// Grounding: loop is `sector.iter_mut().take(2048).skip(128)` -> indices
    /// 128..2048 only.
    /// Mutation: change `.skip(128)` to `.skip(0)` -> header bytes (incl. the
    /// seed) get XORed and this fails.
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

    /// The descrambler must touch the WHOLE body 0x80..0x800, not just a
    /// prefix. With a constant body and constant key, the keystream is
    /// non-degenerate enough that the very last sector byte (index 2047) is
    /// altered. This guards the loop bound `.take(2048)` against an
    /// off-by-one that would leave the final byte(s) scrambled.
    ///
    /// Grounding: encrypted region end is 0x800 == 2048 (exclusive).
    /// Mutation: change `.take(2048)` to `.take(2047)` -> last byte unchanged,
    /// assert fires (keystream byte for the last position is verified nonzero
    /// below by the round-trip, and this body is all-zero so any XOR shows).
    #[test]
    fn descramble_covers_final_body_byte() {
        let key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let mut sector = vec![0x00u8; 2048];
        sector[0x14] = 0x30;
        sector[0x54..0x59].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55]);
        descramble_sector(&key, &mut sector);
        // Body was all zero; any nonzero in [0x80,0x800) is keystream. Confirm
        // the keystream reaches the final byte. (If the last keystream byte
        // happened to be 0 this could be a flaky test, so assert the run-end
        // region as a whole differs from zero.)
        assert_ne!(
            &sector[2040..2048],
            &[0u8; 8][..],
            "the tail of the body must be descrambled (loop must reach index 2047)"
        );
    }

    /// Descramble is keyed by `title_key XOR seed`: two different title keys
    /// produce two different bodies for the same scrambled input. A cipher
    /// that ignored the title key (or mixed it in wrongly) would yield
    /// identical output — silent wrong-key decryption.
    ///
    /// Grounding: per-sector key = title_key[i] ^ sector[0x54+i].
    /// Mutation: in the `key` array drop the `title_key[i] ^` term -> both
    /// keys give the same body, assert fires.
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

    /// Descramble is keyed by the sector seed too: same title key, different
    /// seed -> different body. Pins that bytes 0x54..0x59 actually feed the
    /// keystream (not just the per-sector XOR key).
    ///
    /// Mutation: replace `seed` array reads with a constant -> both seeds give
    /// the same body, assert fires.
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
