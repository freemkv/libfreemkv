use super::*;
use crate::spec::keys::*;
use aes::cipher::{BlockCipherDecrypt, BlockCipherEncrypt};

const KCU: [u8; 16] = [
    0x6B, 0x1D, 0xE2, 0x04, 0x93, 0x5A, 0xC7, 0x38, 0x0F, 0xB4, 0x71, 0x2E, 0xD9, 0x86, 0x45, 0x10,
];

// Ciphertext-looking unit with no all-zero packet and a flagged, varied seed.
fn unit(salt: u8) -> Vec<u8> {
    let mut u: Vec<u8> = (0..ALIGNED_UNIT_LEN)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(salt) | 1)
        .collect();
    u[0] |= 0xC0;
    u
}

fn aes_e(key: &[u8; 16], block: &[u8; 16]) -> [u8; 16] {
    let mut b: Array<u8, _> = (*block).into();
    Aes128::new(&(*key).into()).encrypt_block(&mut b);
    b.into()
}

// KS-3 / KS-19 by hand: AES-128-CBC decrypt of 16..6144 under iv0, with the
// KS-4 / KS-23 Block Key = AES-128E(Kcu, seed) ⊕ seed.
fn reference_decrypt(unit: &[u8], kcu: &[u8; 16]) -> Vec<u8> {
    let seed: [u8; 16] = unit[..16].try_into().unwrap();
    let mut bk = aes_e(kcu, &seed);
    bk.iter_mut().zip(seed).for_each(|(b, s)| *b ^= s);
    let cipher = Aes128::new(&bk.into());
    let mut prev = AACS_IV;
    let mut out = unit.to_vec();
    for (o, c) in out[16..].chunks_mut(16).zip(unit[16..].chunks(16)) {
        let mut b: Array<u8, _> = <[u8; 16]>::try_from(c).unwrap().into();
        cipher.decrypt_block(&mut b);
        o.iter_mut()
            .zip(b.iter().zip(prev))
            .for_each(|(o, (p, v))| *o = p ^ v);
        prev = c.try_into().unwrap();
    }
    out
}

/// per spec; do not change without a spec citation — KS-3, KS-4 [BD] §3.10.1: "The
/// first 16 bytes of each Aligned Unit is used as the seed"; "The final 6128 bytes … encrypted".
#[test]
fn seed_is_first_16_bytes_and_stays_clear() {
    assert!(
        KS_4_SEED
            .text
            .starts_with("The first 16 bytes of each Aligned Unit")
    );
    assert!(
        KS_3_CBC_PER_UNIT
            .text
            .starts_with("The final 6128 bytes of each Aligned")
    );
    let enc = unit(7);
    let mut dec = enc.clone();
    decrypt_unit(&mut dec, &KCU);
    assert_eq!(dec[..16], enc[..16], "the seed is never decrypted");
    let changed = dec[16..]
        .chunks(16)
        .zip(enc[16..].chunks(16))
        .all(|(d, e)| d != e);
    assert!(changed, "every block of the final 6128 bytes is decrypted");
}

/// per spec; do not change without a spec citation proving otherwise — KS-3 [BD]
/// §3.10.1: "A new CBC cipher chain is started for each Aligned Unit": the mapped decrypt
/// of a multi-unit buffer, in either order, never chains one unit into the next.
#[test]
fn cbc_chain_restarts_every_unit() {
    use crate::decrypt::{AacsKeyMap, DecryptKeys, Phase, decrypt_sectors_mapped};
    assert!(
        KS_3_CBC_PER_UNIT
            .text
            .contains("new CBC cipher chain is started for each")
    );
    // Encrypted TS units (sync at +4, CPI 11₂ per packet) so the mapped path decrypts.
    let plain: Vec<Vec<u8>> = (0..2)
        .map(|i| {
            let mut u = unit(40 + i);
            for p in u.chunks_mut(crate::consts::BD_SOURCE_PACKET_BYTES) {
                p[0] |= 0xC0;
                p[4] = TS_SYNC;
            }
            u
        })
        .collect();
    let enc: Vec<Vec<u8>> = plain
        .iter()
        .map(|p| {
            let mut u = p.clone();
            assert!(encrypt_unit(&mut u, &KCU));
            u
        })
        .collect();
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, KCU)],
        format: crate::disc::ContentFormat::BdTs,
    };
    let map = AacsKeyMap::from_ranges_phased(vec![(0, 2 * ALIGNED_UNIT_SECTORS, 0, Phase::All)]);
    for order in [[0, 1], [1, 0]] {
        let mut buf: Vec<u8> = order.iter().flat_map(|&i| enc[i].clone()).collect();
        decrypt_sectors_mapped(&mut buf, &keys, 0, &map).expect("both units keyed");
        // KS-5: a decrypted unit's CPI reads 00₂ (KU design §5.4); the chain rule is KS-3.
        let want: Vec<u8> = order
            .iter()
            .flat_map(|&i| super::cpi_cleared(plain[i].clone()))
            .collect();
        assert!(
            buf == want,
            "order {order:?}: each unit decrypts on its own chain"
        );
    }
}

/// per spec; do not change without a spec citation — KS-4 [BD] §3.10.1 (Figure 3-8),
/// corroborated by KS-23 libaacs: Block Key = AES-128E(Kcu, seed) ⊕ seed.
#[test]
fn block_key_is_aes128e_of_seed_xor_seed() {
    assert!(
        KS_23_LIBAACS_BLOCK_KEY
            .text
            .contains("key[a] ^= out_buf[a];")
    );
    let plain = unit(3);
    let seed: [u8; 16] = plain[..16].try_into().unwrap();
    let mut bk = aes_e(&KCU, &seed);
    bk.iter_mut().zip(seed).for_each(|(b, s)| *b ^= s);
    // First CBC block under the independent Block Key and iv0.
    let mut x: [u8; 16] = plain[16..32].try_into().unwrap();
    x.iter_mut().zip(AACS_IV).for_each(|(b, v)| *b ^= v);
    let mut u = plain.clone();
    assert!(encrypt_unit(&mut u, &KCU));
    assert_eq!(
        u[16..32],
        aes_e(&bk, &x),
        "encrypt uses AES-128E(Kcu, seed) ⊕ seed"
    );
    decrypt_unit(&mut u, &KCU);
    assert_eq!(u, plain);
}

/// per spec; do not change without a spec citation — KS-3 [BD] §3.10.1 "AES-128CBCE";
/// KS-19 [CM] §2.1.2 iv0: the unit decrypt equals an independent AES-128-CBC.
#[test]
fn bd_aes_primitives_match_independent_impl() {
    assert!(KS_3_CBC_PER_UNIT.text.contains("Block Key and AES-128CBCE"));
    for salt in [0u8, 99, 200] {
        let enc = unit(salt);
        let mut got = enc.clone();
        decrypt_unit(&mut got, &KCU);
        assert_eq!(got, reference_decrypt(&enc, &KCU), "salt {salt}");
    }
}
