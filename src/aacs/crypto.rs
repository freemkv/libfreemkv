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

/// AES-128-CBC ENCRYPT in place under the fixed [`AACS_IV`] — the forward
/// direction of [`aes_cbc_decrypt`], and its exact inverse. `[C]` §2.1.2
/// (`AES-128CBCE`).
///
/// Precondition: `data.len()` is a multiple of 16; the assert
/// documents/enforces that contract.
///
/// Constructs the cipher ONCE for the whole slice. Driving this from the
/// single-block [`aes_ecb_encrypt`] instead rebuilds the AES key schedule per
/// 16-byte block, which for a 6144-byte aligned unit is 383 redundant key
/// expansions.
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

/// AES-128-CBC DECRYPT in-place with the fixed AACS IV. `[C]` §2.1.2
/// (`AES-128CBCD`).
///
/// Precondition: `data.len()` is a multiple of 16. Any trailing partial
/// block is silently ignored; all callers pass aligned regions (6128 and
/// 2032 bytes), and the assert documents/enforces that contract.
///
/// (This doc block was orphaned onto `aes_cbc_encrypt` above when that function
/// was inserted directly after it with no separating blank line, so rustdoc
/// rendered the crate's only forward-direction AACS primitive as "decrypt" and
/// cited the spec's DECRYPT clause for it, while this function had no doc at
/// all. `encrypt_unit_is_the_exact_inverse_of_decrypt_unit` in `content.rs` pins
/// the directions behaviourally so a maintainer 'fixing' the contradiction by
/// swapping the two bodies fails the suite instead of shipping a second
/// decryptor behind an already-set encrypted flag.)
pub(crate) fn aes_cbc_decrypt(key: &[u8; 16], data: &mut [u8]) {
    debug_assert!(
        data.len().is_multiple_of(16),
        "aes_cbc_decrypt requires a block-aligned slice"
    );
    cbc_decrypt_blocks(&new_cipher(key), data);
}

/// AES-128-CBC decrypt in place under the fixed [`AACS_IV`] with an ALREADY
/// EXPANDED key schedule.
///
/// Split out of [`aes_cbc_decrypt`] so a caller that decrypts several regions
/// under one loop-invariant key expands the schedule once. `decrypt_bus`
/// ([`super::content::decrypt_bus`]) is that caller: bus encryption
/// (`[C]` §4.2 / the AACS 2.0 Read Data Key) covers bytes 16..2048 of EVERY
/// 2048-byte sector, so a 6144-byte aligned unit is three regions under one
/// `read_data_key` — three key schedules where one suffices, on the per-unit
/// decrypt hot path of a whole 90 GB read.
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

/// AES-G(x1, x2) = AES-128D(x1, x2) XOR x2. `[C]` §2.1.3 (note: uses AES-128**D**).
///
/// The Media Key Variant chain uses AES-G to derive both the variant
/// number (`Kvn = AES-G(Kp, Nonce)`) and the Volume Unique Key
/// (`Kvu = AES-G(Km, VID)`). See [`super::derive::derive_vuk`] for the
/// classical VUK form — the math is identical, this exposes it as a
/// neutral primitive for the variant chain.
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

/// AACS-G3: derive a subkey from a parent key. `[C]` §3.2.2 (Triple AES Generator:
/// left=`D(k,s0)⊕s0` inc 0, pk=`D(k,s0+1)⊕(s0+1)` inc 1, right=`D(k,s0+2)⊕(s0+2)` inc 2).
/// `seed[15] += inc`, then AES-DEC(key, seed) XOR seed.
///
/// Shared with [`super::variant`] (its variant chain runs the same SD
/// tree); a single definition keeps the two walks byte-identical.
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

    /// The AACS-G3 seed `s0`, transcribed independently from `[C]` §3.2.2 rather
    /// than read from [`AESG3_SEED`] — a test that sourced the seed from the
    /// production constant would assert that constant against itself and would
    /// still pass if it were edited.
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

    /// `aesg3` is the node function of the AACS subset-difference tree: every
    /// Processing Key the DK walk produces (`aesg3(node_key, 1)`) and every
    /// descent step (`aesg3(., 0)` / `aesg3(., 2)`) is one call. A body that
    /// returned a fixed block would make every device key in the crate derive
    /// the SAME Processing Key, and a `^` that became `|` or `&` would derive a
    /// wrong-but-plausible one — in both cases the MKB walk simply stops
    /// finding Media Keys, with no error to say why.
    ///
    /// Pinned through the spec relation rather than a re-implementation:
    /// `[C]` §3.2.2 defines `AES-G3` as `AES-128D(k, s) XOR s` for
    /// `s = s0 + inc` (added into the last seed byte), so applying the
    /// FORWARD primitive [`aes_ecb_encrypt`] — a different function from the
    /// one under test — to `aesg3(k, inc) XOR s` must reproduce `s` exactly.
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

    /// The Triple Generator's three outputs (`[C]` §3.2.2: left = inc 0, the
    /// Processing Key = inc 1, right = inc 2) are the two child node keys and
    /// the Processing Key of ONE tree node. They must be three different keys —
    /// if `inc` were ignored, a descent would revisit its own parent and the
    /// walk would derive the same key at every level of the tree.
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
