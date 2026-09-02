//! AACS common cryptographic primitives — `[C]` Chapter 2 / §3.2.2.
//!
//! Source: `[C]` = AACS Introduction and Common Cryptographic Elements Book,
//! Rev 0.953. The shared low-level building blocks — AES-128 ECB E/D, AES-G,
//! the AES-G3 Triple Generator, AES-CBC decrypt — and their fixed constants
//! (`iv0`, `s0`). Used by every AACS generation; relocated here so the
//! primitives live in one place instead of being scattered across the
//! content / keys / variant modules.

use aes::Aes128;
use aes::cipher::{Array, BlockCipherDecrypt, BlockCipherEncrypt, KeyInit};

/// Fixed IV used by AACS for all AES-CBC operations. `[C]` §2.1.2 (default CBC IV, `iv0`).
pub(crate) const AACS_IV: [u8; 16] = [
    0x0B, 0xA0, 0xF8, 0xDD, 0xFE, 0xA6, 0x1F, 0xB3, 0xD8, 0xDF, 0x9F, 0x56, 0x6A, 0x05, 0x0F, 0x78,
];

// Per-thread count of AES-128 key schedules built through `new_cipher`.
// Test-only: lets tests assert the CBC decrypt hot path builds one schedule
// per loop-invariant key. Thread-local, not atomic, since `cargo test` runs concurrently.
#[cfg(test)]
thread_local! {
    pub(crate) static KEY_EXPANSIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Build an AES-128 key schedule for a caller that will drive
/// [`cbc_decrypt_blocks`] over several regions under one key.
pub(crate) fn new_cipher_for(key: &[u8; 16]) -> Aes128 {
    new_cipher(key)
}

/// Build an AES-128 key schedule. The single construction site for the CBC
/// helpers, so `KEY_EXPANSIONS` can count them under test.
fn new_cipher(key: &[u8; 16]) -> Aes128 {
    #[cfg(test)]
    KEY_EXPANSIONS.with(|c| c.set(c.get() + 1));
    Aes128::new(&(*key).into())
}

/// AES-128-ECB encrypt a single 16-byte block. `[C]` §2.1.1 (`AES-128E`).
pub(crate) fn aes_ecb_encrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
    let cipher = Aes128::new(&(*key).into());
    let mut block: Array<u8, _> = (*data).into();
    cipher.encrypt_block(&mut block);
    let mut out = [0u8; 16];
    out.copy_from_slice(&block);
    out
}

/// AES-128-ECB decrypt a single 16-byte block. `[C]` §2.1.1 (`AES-128D`).
pub(crate) fn aes_ecb_decrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
    let cipher = Aes128::new(&(*key).into());
    let mut block: Array<u8, _> = (*data).into();
    cipher.decrypt_block(&mut block);
    let mut out = [0u8; 16];
    out.copy_from_slice(&block);
    out
}

// AES-128-CBC encrypt in place under AACS_IV. Forward direction / exact
// inverse of aes_cbc_decrypt (`[C]` §2.1.2, AES-128CBCE).
// See docs/aacs-crypto.md — aes_cbc_encrypt (precondition, single key schedule).
pub(crate) fn aes_cbc_encrypt(key: &[u8; 16], data: &mut [u8]) {
    debug_assert!(
        data.len().is_multiple_of(16),
        "aes_cbc_encrypt requires a block-aligned slice"
    );
    let cipher = new_cipher(key);
    let num_blocks = data.len() / 16;
    let mut prev = AACS_IV;
    // Forward order: each block is XORed with the PRECEDING ciphertext block.
    for i in 0..num_blocks {
        let offset = i * 16;
        let mut block = [0u8; 16];
        for j in 0..16 {
            block[j] = data[offset + j] ^ prev[j];
        }
        let mut ga: Array<u8, _> = block.into();
        cipher.encrypt_block(&mut ga);
        data[offset..offset + 16].copy_from_slice(&ga);
        prev.copy_from_slice(&ga);
    }
}

// AES-128-CBC decrypt in-place under AACS_IV (`[C]` §2.1.2, AES-128CBCD).
// See docs/aacs-crypto.md — aes_cbc_decrypt (precondition, doc-orphaning history).
pub(crate) fn aes_cbc_decrypt(key: &[u8; 16], data: &mut [u8]) {
    debug_assert!(
        data.len().is_multiple_of(16),
        "aes_cbc_decrypt requires a block-aligned slice"
    );
    cbc_decrypt_blocks(&new_cipher(key), data);
}

// AES-128-CBC decrypt in place under AACS_IV with an already-expanded key
// schedule. Split out of aes_cbc_decrypt for callers that share one key
// across several regions. See docs/aacs-crypto.md — cbc_decrypt_blocks.
pub(crate) fn cbc_decrypt_blocks(cipher: &Aes128, data: &mut [u8]) {
    let num_blocks = data.len() / 16;
    // Process blocks in reverse to avoid clobbering ciphertext needed for XOR
    for i in (0..num_blocks).rev() {
        let offset = i * 16;
        let prev = if i == 0 {
            AACS_IV
        } else {
            let mut p = [0u8; 16];
            p.copy_from_slice(&data[(i - 1) * 16..i * 16]);
            p
        };
        let mut chunk = [0u8; 16];
        chunk.copy_from_slice(&data[offset..offset + 16]);
        let mut block: Array<u8, _> = chunk.into();
        cipher.decrypt_block(&mut block);
        for j in 0..16 {
            data[offset + j] = block[j] ^ prev[j];
        }
    }
}

// AES-G(x1, x2) = AES-128D(x1, x2) XOR x2 (`[C]` §2.1.3, uses AES-128D).
// Used by the Media Key Variant chain to derive Kvn and Kvu.
// See docs/aacs-crypto.md — aes_g.
pub(crate) fn aes_g(x1: &[u8; 16], x2: &[u8; 16]) -> [u8; 16] {
    let mut out = aes_ecb_decrypt(x1, x2);
    for i in 0..16 {
        out[i] ^= x2[i];
    }
    out
}

/// AACS-G3 seed constant (`s0`). `[C]` §3.2.2.
pub(crate) const AESG3_SEED: [u8; 16] = [
    0x7B, 0x10, 0x3C, 0x5D, 0xCB, 0x08, 0xC4, 0xE5, 0x1A, 0x27, 0xB0, 0x17, 0x99, 0x05, 0x3B, 0xD9,
];

// AACS-G3: derive a subkey from a parent key (`[C]` §3.2.2, Triple AES
// Generator). Shared with super::variant so both SD-tree walks stay
// byte-identical. See docs/aacs-crypto.md — aesg3.
pub(crate) fn aesg3(key: &[u8; 16], inc: u8) -> [u8; 16] {
    let mut seed = AESG3_SEED;
    seed[15] = seed[15].wrapping_add(inc);
    let mut out = aes_ecb_decrypt(key, &seed);
    for i in 0..16 {
        out[i] ^= seed[i];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // s0 transcribed independently from `[C]` §3.2.2, not read from AESG3_SEED,
    // so this can't assert the production constant against itself.
    // See docs/aacs-crypto.md — test: S0.
    const S0: [u8; 16] = [
        0x7B, 0x10, 0x3C, 0x5D, 0xCB, 0x08, 0xC4, 0xE5, 0x1A, 0x27, 0xB0, 0x17, 0x99, 0x05, 0x3B,
        0xD9,
    ];

    /// An arbitrary non-degenerate key. Nothing about it is secret or special;
    /// the AES-G3 relation holds for every key, and a constant-returning body
    /// cannot satisfy it for any.
    const K: [u8; 16] = [
        0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78, 0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2, 0xE1,
        0xF0,
    ];

    // aesg3 is the SD-tree node function; a wrong `^` or a constant body
    // would derive a plausible-but-wrong Processing Key. Pinned via the spec
    // relation (see docs/aacs-crypto.md for the full derivation).
    #[test]
    fn aesg3_inverts_to_the_spec_seed_under_aes_encrypt() {
        for inc in 0u8..=2 {
            let mut seed = S0;
            seed[15] = seed[15].wrapping_add(inc);

            let out = aesg3(&K, inc);

            // out == AES-128D(K, seed) XOR seed, so out XOR seed is the raw
            // decryption and re-encrypting it must land back on the seed.
            let mut pre = [0u8; 16];
            for i in 0..16 {
                pre[i] = out[i] ^ seed[i];
            }
            assert_eq!(
                aes_ecb_encrypt(&K, &pre),
                seed,
                "AES-G3 inc={inc} must satisfy out = AES-128D(k, s0+inc) XOR (s0+inc)"
            );
        }
    }

    // The Triple Generator's three outputs are one node's two children plus
    // its Processing Key; if `inc` were ignored a descent would revisit its
    // own parent. See docs/aacs-crypto.md — test: three distinct subkeys.
    #[test]
    fn aesg3_yields_three_distinct_subkeys_for_the_three_increments() {
        let left = aesg3(&K, 0);
        let pk = aesg3(&K, 1);
        let right = aesg3(&K, 2);
        assert_ne!(left, pk, "left child and Processing Key must differ");
        assert_ne!(pk, right, "Processing Key and right child must differ");
        assert_ne!(left, right, "left and right children must differ");
    }

    /// Distinct parent keys must yield distinct subkeys — the tree would
    /// collapse otherwise.
    #[test]
    fn aesg3_separates_distinct_parent_keys() {
        let mut other = K;
        other[0] ^= 0x01;
        assert_ne!(aesg3(&K, 1), aesg3(&other, 1));
    }
}
