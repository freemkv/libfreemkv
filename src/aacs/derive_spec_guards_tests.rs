use super::*;
use crate::spec::keys::KS_15_KCU_WRAP;
use crate::test_util::unit_key_ro;
use aes::Aes128;
use aes::cipher::{Array, BlockCipherEncrypt, KeyInit};

fn aes_e(key: &[u8; 16], block: &[u8; 16]) -> [u8; 16] {
    let mut b: Array<u8, _> = (*block).into();
    Aes128::new(&(*key).into()).encrypt_block(&mut b);
    b.into()
}

/// per spec; do not change without a spec citation — KS-15 [BD] §3.9.3: "The CPS Unit
/// Key is encrypted as follows: AES-128E( Kvu, Kcu )", so Kcu = AES-128D(Kvu, stored).
#[test]
fn kcu_is_aes128d_of_encrypted_key_under_kvu() {
    assert!(KS_15_KCU_WRAP.text.ends_with("AES-128E( Kvu, Kcu )"));
    let kvu: [u8; 16] = core::array::from_fn(|i| 0x30 + i as u8 * 7);
    let kcu = [[0x5Au8; 16], core::array::from_fn(|i| i as u8)];
    let wrapped = [aes_e(&kvu, &kcu[0]), aes_e(&kvu, &kcu[1])];
    let inf = unit_key_ro(AacsVersion::V10, &wrapped, &[1, 2]);
    let ukf = parse_unit_key_ro(&inf, AacsVersion::V10).unwrap();
    assert_eq!(derive_unit_keys(&ukf, &kvu), vec![(1, kcu[0]), (2, kcu[1])]);
    assert_eq!(decrypt_unit_key(&kvu, &wrapped[1]), kcu[1]);
    assert_ne!(
        derive_unit_keys(&ukf, &kcu[0])[0].1,
        kcu[0],
        "Kvu is the wrapping key"
    );
}
