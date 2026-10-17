//! AES-128 block operations supplied by the RustCrypto AES dependency.

use aes::Aes128;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit};

pub(crate) fn aes_ecb_encrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
    let cipher = Aes128::new(&(*key).into());
    let mut block = (*data).into();
    cipher.encrypt_block(&mut block);
    block.into()
}

pub(crate) fn aes_ecb_decrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
    let cipher = Aes128::new(&(*key).into());
    let mut block = (*data).into();
    cipher.decrypt_block(&mut block);
    block.into()
}
