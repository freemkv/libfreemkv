//! AACS common cryptographic primitives — [C] Chapter 2 / §3.2.2.
//!
//! Source: `[C]` = AACS Introduction and Common Cryptographic Elements Book,
//! Rev 0.953. The shared low-level building blocks — AES-128 ECB E/D, AES-G,
//! the AES-G3 Triple Generator, AES-CBC decrypt — and their fixed constants
//! (`iv0`, `s0`). Used by every AACS generation; relocated here so the
//! primitives live in one place instead of being scattered across the
//! content / keys / variant modules.

use aes::Aes128;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit, generic_array::GenericArray};

/// Fixed IV used by AACS for all AES-CBC operations. [C] §2.1.2 (default CBC IV, `iv0`).
pub(crate) const AACS_IV: [u8; 16] = [
    0x0B, 0xA0, 0xF8, 0xDD, 0xFE, 0xA6, 0x1F, 0xB3, 0xD8, 0xDF, 0x9F, 0x56, 0x6A, 0x05, 0x0F, 0x78,
];

/// AES-128-ECB encrypt a single 16-byte block. [C] §2.1.1 (`AES-128E`).
pub(crate) fn aes_ecb_encrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
    let cipher = Aes128::new(GenericArray::from_slice(key));
    let mut block = GenericArray::clone_from_slice(data);
    cipher.encrypt_block(&mut block);
    let mut out = [0u8; 16];
    out.copy_from_slice(&block);
    out
}

/// AES-128-ECB decrypt a single 16-byte block. [C] §2.1.1 (`AES-128D`).
pub(crate) fn aes_ecb_decrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
    let cipher = Aes128::new(GenericArray::from_slice(key));
    let mut block = GenericArray::clone_from_slice(data);
    cipher.decrypt_block(&mut block);
    let mut out = [0u8; 16];
    out.copy_from_slice(&block);
    out
}

/// AES-128-CBC decrypt in-place with the fixed AACS IV. [C] §2.1.2 (`AES-128CBCD`).
///
/// Precondition: `data.len()` is a multiple of 16. Any trailing partial
/// block is silently ignored; all callers pass aligned regions (6128 and
/// 2032 bytes), and the assert documents/enforces that contract.
pub(crate) fn aes_cbc_decrypt(key: &[u8; 16], data: &mut [u8]) {
    debug_assert!(
        data.len() % 16 == 0,
        "aes_cbc_decrypt requires a block-aligned slice"
    );
    let cipher = Aes128::new(GenericArray::from_slice(key));
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
        let mut block = GenericArray::clone_from_slice(&data[offset..offset + 16]);
        cipher.decrypt_block(&mut block);
        for j in 0..16 {
            data[offset + j] = block[j] ^ prev[j];
        }
    }
}

/// AES-G(x1, x2) = AES-128D(x1, x2) XOR x2. [C] §2.1.3 (note: uses AES-128**D**).
///
/// The Media Key Variant chain uses AES-G to derive both the variant
/// number (`Kvn = AES-G(Kp, Nonce)`) and the Volume Unique Key
/// (`Kvu = AES-G(Km, VID)`). See [`super::keys::derive_vuk`] for the
/// classical VUK form — the math is identical, this exposes it as a
/// neutral primitive for the variant chain.
pub(crate) fn aes_g(x1: &[u8; 16], x2: &[u8; 16]) -> [u8; 16] {
    let mut out = aes_ecb_decrypt(x1, x2);
    for i in 0..16 {
        out[i] ^= x2[i];
    }
    out
}

/// AACS-G3 seed constant (`s0`). [C] §3.2.2.
pub(crate) const AESG3_SEED: [u8; 16] = [
    0x7B, 0x10, 0x3C, 0x5D, 0xCB, 0x08, 0xC4, 0xE5, 0x1A, 0x27, 0xB0, 0x17, 0x99, 0x05, 0x3B, 0xD9,
];

/// AACS-G3: derive a subkey from a parent key. [C] §3.2.2 (Triple AES Generator:
/// left=`D(k,s0)⊕s0` inc 0, pk=`D(k,s0+1)⊕(s0+1)` inc 1, right=`D(k,s0+2)⊕(s0+2)` inc 2).
/// seed[15] += inc, then AES-DEC(key, seed) XOR seed.
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
