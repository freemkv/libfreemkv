//! CSS content cipher (constants in `super::tables`). Two table-driven
//! linear-feedback circuits:
//! - **LFSR1** — 17-bit register seeded from `key[0..2] XOR seed[0..2]`,
//!   stepped through `TAB2`/`TAB3`/`TAB5`.
//! - **LFSR0** — 24-bit register seeded from `key[2..5] XOR seed[2..5]`,
//!   stepped through a feedback polynomial and `TAB4`.
//!
//! Output byte = sum-with-carry of both registers; body byte recovered as `plain = TAB1[cipher]
//! ^ keystream`.

use super::tables::{TAB1, TAB2, TAB3, TAB4, TAB5};

/// Descramble a CSS-encrypted DVD sector in place.
///
/// Seeded from `title_key XOR sector_seed` (bytes `0x54..0x59`); transforms only
/// the body `0x80..0x800`: `body[i] = TAB1[body[i]] ^ (keystream & 0xff)`.
/// Flag bits 4-5 at byte `0x14` are cleared afterwards.
///
/// No-op if `sector.len() < 2048` or the scramble flag bits are already zero.
#[doc(hidden)]
pub fn descramble_sector(title_key: &[u8; 5], sector: &mut [u8]) {
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

// Exact inverse of descramble_sector, for test use only (builds known ciphertext).
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
#[path = "lfsr_tests.rs"]
mod tests;
