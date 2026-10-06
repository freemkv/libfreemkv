use super::*;

// s0 transcribed independently from `[C]` §3.2.2, not read from AESG3_SEED, so this can't
// assert the production constant against itself.
const S0: [u8; 16] = [
    0x7B, 0x10, 0x3C, 0x5D, 0xCB, 0x08, 0xC4, 0xE5, 0x1A, 0x27, 0xB0, 0x17, 0x99, 0x05, 0x3B, 0xD9,
];

/// An arbitrary non-degenerate key. Nothing about it is secret or special;
/// the AES-G3 relation holds for every key, and a constant-returning body
/// cannot satisfy it for any.
const K: [u8; 16] = [
    0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78, 0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2, 0xE1, 0xF0,
];

// aesg3 is the SD-tree node function; a wrong `^` or a constant body would derive a
// plausible-but-wrong Processing Key. Pinned via the spec relation.
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

// The Triple Generator's three outputs are one node's two children plus its Processing Key;
// if `inc` were ignored a descent would revisit its own parent.
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

// The batched CBC decrypt inverts the (block-at-a-time) CBC encrypt at every
// batch-boundary shape: one block, exactly one batch, a batch plus one, and a
// whole de-bussed sector body.
#[test]
fn batched_cbc_decrypt_inverts_cbc_encrypt_across_batch_boundaries() {
    for blocks in [1usize, 31, 32, 33, 64, 383] {
        let plain: Vec<u8> = (0..blocks * 16).map(|i| (i * 7 + 3) as u8).collect();
        let mut data = plain.clone();
        aes_cbc_encrypt(&K, &mut data);
        assert_ne!(data, plain);
        aes_cbc_decrypt(&K, &mut data);
        assert_eq!(data, plain, "{blocks} blocks must round-trip");
    }
}
